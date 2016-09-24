{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
module DynamoDbEventStore.Streams
  (streamEventsProducer
  ,globalEventKeysProducer
  ,globalEventsProducer
  ,writeEvent
  ,EventWriteResult(..))
  where

import DynamoDbEventStore.ProjectPrelude
import Safe.Exact
import Data.Foldable
import DynamoDbEventStore.Types
    (GlobalFeedPosition(..)
    ,EventKey(..)
    ,PageKey(..)
    ,EventStoreActionError(..)
    ,FeedEntry(..)
    ,QueryDirection(..)
    ,EventId(..)
    ,StreamId(..)
    ,RecordedEvent(..)
    ,DynamoWriteResult(..))
import qualified DynamoDbEventStore.Storage.HeadItem as HeadItem
import qualified DynamoDbEventStore.Storage.StreamItem as StreamItem
import qualified DynamoDbEventStore.Storage.GlobalStreamItem as GlobalStreamItem
import qualified Pipes.Prelude                         as P
import qualified Data.List.NonEmpty                    as NonEmpty
import qualified Data.ByteString.Lazy                  as BL

import DynamoDbEventStore.EventStoreCommands (MonadEsDsl)
import           DynamoDbEventStore.Storage.StreamItem (StreamEntry(..), EventEntry(..),eventTypeToText, unEventTime,streamEntryProducer)

data EventWriteResult = WriteSuccess | WrongExpectedVersion | EventExists | WriteError deriving (Eq, Show)

toRecordedEvent :: (MonadEsDsl m) => StreamEntry -> m (NonEmpty RecordedEvent)
toRecordedEvent StreamEntry{..} = do
  let eventEntriesWithEventNumber = NonEmpty.zip (streamEntryFirstEventNumber :| [streamEntryFirstEventNumber + 1 ..]) streamEntryEventEntries
  let (StreamId streamId) = streamEntryStreamId
  let buildEvent (eventNumber, EventEntry{..}) = RecordedEvent streamId eventNumber (BL.toStrict eventEntryData) (eventTypeToText eventEntryType) (unEventTime eventEntryCreated) eventEntryEventId eventEntryIsJson
  let recordedEvents = buildEvent <$> eventEntriesWithEventNumber
  return recordedEvents

toRecordedEventBackward :: (MonadEsDsl m) => StreamEntry -> m (NonEmpty RecordedEvent)
toRecordedEventBackward readResult = NonEmpty.reverse <$> toRecordedEvent readResult

streamItemToRecordedEventPipe :: (MonadEsDsl m) => Pipe StreamEntry RecordedEvent m ()
streamItemToRecordedEventPipe = forever $ do
  streamItem <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEvent streamItem
  forM_ (NonEmpty.toList recordedEvents) yield

streamItemToRecordedEventBackwardPipe :: (MonadEsDsl m) => Pipe StreamEntry RecordedEvent m ()
streamItemToRecordedEventBackwardPipe = forever $ do
  streamItem <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEventBackward streamItem
  forM_ (NonEmpty.toList recordedEvents) yield

streamEventsProducer :: (MonadEsDsl m, MonadError EventStoreActionError m) => QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent m ()
streamEventsProducer QueryDirectionBackward streamId lastEvent batchSize =
  let
    maxEventToRetrieve = (+1) <$> lastEvent
  in
    streamEntryProducer QueryDirectionBackward streamId maxEventToRetrieve batchSize
    >-> streamItemToRecordedEventBackwardPipe
    >-> filterLastEvent lastEvent
  where
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber)
streamEventsProducer QueryDirectionForward streamId Nothing batchSize =
  streamEntryProducer QueryDirectionForward streamId Nothing batchSize >-> streamItemToRecordedEventPipe
streamEventsProducer QueryDirectionForward streamId firstEvent batchSize =
  (streamEntryProducer QueryDirectionBackward streamId ((+1) <$> firstEvent) 1 >-> streamItemToRecordedEventPipe -- first page backward
     >>
     streamEntryProducer QueryDirectionForward streamId firstEvent batchSize >-> streamItemToRecordedEventPipe) -- rest of the pages
    >->
    filterFirstEvent firstEvent
  where
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber)

getGlobalFeedBackward :: (MonadEsDsl m, MonadError EventStoreActionError m) => Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ()
getGlobalFeedBackward Nothing = do
  lastKnownPage <- lift HeadItem.getLastFullPage
  let lastKnownPage' = fromMaybe (PageKey 0) lastKnownPage
  lastItem <- lift $ P.last (getPageItemsForward lastKnownPage')
  let lastPosition = fst <$> lastItem
  maybe (return ()) (getGlobalFeedBackward . Just) lastPosition
getGlobalFeedBackward (Just (position@GlobalFeedPosition{..})) =
  getFirstPageBackward position >> getPageItemsBackward (globalFeedPositionPage - 1)

getPagesBackward :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> Producer [(GlobalFeedPosition,EventKey)] m ()
getPagesBackward (PageKey (-1)) = return ()
getPagesBackward page = do
  result <- lift $ GlobalStreamItem.readPage page
  _ <- case result of (Just entries) -> yield (globalFeedItemToEventKeys entries)
                      Nothing        -> lift $ throwError (EventStoreActionErrorInvalidGlobalFeedPage page)
  getPagesBackward (page - 1)

globalFeedItemToEventKeys :: GlobalStreamItem.GlobalFeedItem -> [(GlobalFeedPosition, EventKey)]
globalFeedItemToEventKeys GlobalStreamItem.GlobalFeedItem{..} =
  let eventKeys = join $ feedEntryToEventKeys <$> toList globalFeedItemFeedEntries
  in zip (GlobalFeedPosition globalFeedItemPageKey <$> [0..]) eventKeys

globalEventKeysProducer :: (MonadEsDsl m, MonadError EventStoreActionError m) => QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ()
globalEventKeysProducer QueryDirectionBackward startPosition =
  getGlobalFeedBackward startPosition
globalEventKeysProducer QueryDirectionForward startPosition =
  let
    startPage = fromMaybe 0 (globalFeedPositionPage <$> startPosition)
  in
    getPageItemsForward startPage
    >-> filterFirstEvent startPosition
  where
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just position) = P.filter ((> position) . fst)

lookupEvent :: (MonadEsDsl m, MonadError EventStoreActionError m) => StreamId -> Int64 -> m (Maybe RecordedEvent)
lookupEvent streamId eventNumber =
  P.head $
    streamEventsProducer QueryDirectionBackward streamId (Just eventNumber) 1
    >->
    P.dropWhile ((/= eventNumber). recordedEventNumber)

lookupEventKey :: (MonadEsDsl m, MonadError EventStoreActionError m) => Pipe (GlobalFeedPosition, EventKey) (GlobalFeedPosition, RecordedEvent) m ()
lookupEventKey = forever $ do
  (position, eventKey@(EventKey(streamId, eventNumber))) <- await
  (maybeRecordedEvent :: Maybe RecordedEvent) <- lift $ lookupEvent streamId eventNumber
  let withPosition = (\e -> (position, e)) <$> maybeRecordedEvent
  maybe (throwError $ EventStoreActionErrorCouldNotFindEvent eventKey) yield withPosition

globalEventsProducer :: (MonadEsDsl m, MonadError EventStoreActionError m) => QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, RecordedEvent) m ()
globalEventsProducer direction startPosition =
  globalEventKeysProducer direction startPosition
  >-> lookupEventKey

feedEntryToEventKeys :: FeedEntry -> [EventKey]
feedEntryToEventKeys FeedEntry { feedEntryStream = streamId, feedEntryNumber = eventNumber, feedEntryCount = entryCount } =
  (\number -> EventKey(streamId, number)) <$> take entryCount [eventNumber..]

getPageItemsBackward :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> Producer (GlobalFeedPosition, EventKey) m ()
getPageItemsBackward startPage =
  getPagesBackward startPage >-> readResultToEventKeys
  where
    readResultToEventKeys = forever $
      (reverse <$> await) >>= mapM_ yield

getFirstPageBackward :: (MonadEsDsl m, MonadError EventStoreActionError m) => GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ()
getFirstPageBackward position@GlobalFeedPosition{..} = do
  items <- lift $ GlobalStreamItem.readPage globalFeedPositionPage
  let itemsBeforePosition = (globalFeedItemToEventKeys <$> items) >>= takeExactMay (globalFeedPositionOffset + 1)
  maybe notFoundError yieldItemsInReverse itemsBeforePosition
  where
    notFoundError = lift $ throwError (EventStoreActionErrorInvalidGlobalFeedPosition position)
    yieldItemsInReverse = mapM_ yield . reverse

getPagesForward :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> Producer [(GlobalFeedPosition,EventKey)] m ()
getPagesForward startPage = do
  result <- lift $ GlobalStreamItem.readPage startPage
  case result of (Just entries) -> yield (globalFeedItemToEventKeys entries) >> getPagesForward (startPage + 1)
                 Nothing        -> return ()

getPageItemsForward :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> Producer (GlobalFeedPosition, EventKey) m ()
getPageItemsForward startPage =
  getPagesForward startPage >-> readResultToEventKeys
  where
    readResultToEventKeys = forever $
      await >>= mapM_ yield

ensureExpectedVersion :: (MonadEsDsl m, MonadError EventStoreActionError m) => StreamId -> Int64 -> m Bool
ensureExpectedVersion _streamId (-1) = return True
ensureExpectedVersion _streamId 0 = return True
ensureExpectedVersion streamId expectedEventNumber =
  checkEventNumber <$> StreamItem.getLastStreamItem streamId
  where
    checkEventNumber Nothing = False
    checkEventNumber (Just StreamEntry {..}) =
      let lastEventNumber = streamEntryFirstEventNumber + fromIntegral (length streamEntryEventEntries) - 1
      in lastEventNumber == expectedEventNumber

writeEvent :: (MonadEsDsl m, MonadError EventStoreActionError m) => StreamId -> Maybe Int64 -> NonEmpty EventEntry -> m EventWriteResult
writeEvent (StreamId sId) ev eventEntries = do
  let eventId = (eventEntryEventId . NonEmpty.head) eventEntries
  dynamoKeyOrError <- getDynamoKey sId ev eventId
  case dynamoKeyOrError of Left a -> return a
                           Right dynamoKey -> writeMyEvent dynamoKey
  where
    writeMyEvent :: (MonadEsDsl m, MonadError EventStoreActionError m) => Int64 -> m EventWriteResult
    writeMyEvent eventNumber = do
      let streamEntry = StreamEntry {
            streamEntryStreamId = StreamId sId,
            streamEntryFirstEventNumber = eventNumber,
            streamEntryEventEntries = eventEntries,
            streamEntryNeedsPaging = True }
      writeResult <- StreamItem.writeStreamItem streamEntry
      return $ toEventResult writeResult
    getDynamoKey :: (MonadEsDsl m, MonadError EventStoreActionError m) => Text -> Maybe Int64 -> EventId -> m (Either EventWriteResult Int64)
    getDynamoKey streamId Nothing eventId = do
      lastEvent <- P.head $ streamEventsProducer QueryDirectionBackward (StreamId streamId) Nothing 1
      let lastEventNumber = maybe (-1) recordedEventNumber lastEvent
      let lastEventIdIsNotTheSame = maybe True ((/= eventId) . recordedEventId) lastEvent
      if lastEventIdIsNotTheSame then
        let eventVersion = lastEventNumber + 1
        in return . Right $ eventVersion
      else return $ Left WriteSuccess
    getDynamoKey streamId (Just expectedVersion) _eventId = do
      expectedVersionOk <- ensureExpectedVersion (StreamId streamId) expectedVersion
      if expectedVersionOk then do
        let eventVersion = expectedVersion + 1
        return . Right $ eventVersion
      else
        return $ Left WrongExpectedVersion
    toEventResult :: DynamoWriteResult -> EventWriteResult
    toEventResult DynamoWriteSuccess = WriteSuccess
    toEventResult DynamoWriteFailure = WriteError
    toEventResult DynamoWriteWrongVersion = EventExists
