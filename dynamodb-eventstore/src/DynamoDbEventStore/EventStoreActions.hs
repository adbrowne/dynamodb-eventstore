{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module DynamoDbEventStore.EventStoreActions(
  ReadStreamRequest(..),
  ReadEventRequest(..),
  ReadAllRequest(..),
  PostEventRequest(..),
  EventType(..),
  EventTime(..),
  unEventTime,
  EventEntry(..),
  EventStoreAction(..),
  EventWriteResult(..),
  PostEventResult(..),
  ReadStreamResult(..),
  ReadAllResult(..),
  ReadEventResult(..),
  FeedDirection(..),
  StreamResult(..),
  StreamOffset,
  GlobalStreamResult(..),
  GlobalStreamOffset,
  EventStartPosition(..),
  GlobalStartPosition(..),
  GlobalFeedPosition(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadEventRequestProgram,
  getReadAllRequestProgram,
  recordedEventProducerBackward) where

import           BasicPrelude
import           Control.Monad.Except
import           Data.Foldable
import qualified Data.ByteString.Lazy                  as BL
import           Data.List.NonEmpty                    (NonEmpty (..))
import qualified Data.List.NonEmpty                    as NonEmpty
import qualified DynamoDbEventStore.Storage.HeadItem as HeadItem
import           DynamoDbEventStore.Storage.GlobalStreamItem (readPage, GlobalFeedItem(..))
import           DynamoDbEventStore.Storage.StreamItem (StreamEntry(..), EventEntry(..),eventTypeToText, EventType(..),EventTime(..),unEventTime,streamEntryProducer,writeStreamItem, getLastStreamItem)
import           DynamoDbEventStore.EventStoreCommands hiding (readField)
import           DynamoDbEventStore.GlobalFeedWriter   (DynamoCmdWithErrors)
import           DynamoDbEventStore.Types
import           GHC.Natural
import           Pipes                                 hiding (ListT, runListT)
import qualified Pipes.Prelude                         as P
import           Safe
import           Safe.Exact
import qualified Test.QuickCheck                       as QC
import           Test.QuickCheck.Instances             ()

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadEvent ReadEventRequest |
  ReadAll ReadAllRequest deriving (Show)

data EventStartPosition = EventStartHead | EventStartPosition Int64 deriving (Show, Eq)
data GlobalStartPosition = GlobalStartHead | GlobalStartPosition GlobalFeedPosition deriving (Show, Eq)

type StreamOffset = (FeedDirection, EventStartPosition, Natural)

data StreamResult = StreamResult {
    streamResultEvents   :: [RecordedEvent]
  , streamResultFirst    :: Maybe StreamOffset
  , streamResultNext     :: Maybe StreamOffset
  , streamResultPrevious :: Maybe StreamOffset
  , streamResultLast     :: Maybe StreamOffset
} deriving Show

type GlobalStreamOffset = (FeedDirection, GlobalStartPosition, Natural)

data GlobalStreamResult = GlobalStreamResult {
    globalStreamResultEvents   :: [RecordedEvent]
  , globalStreamResultFirst    :: Maybe GlobalStreamOffset
  , globalStreamResultNext     :: Maybe GlobalStreamOffset
  , globalStreamResultPrevious :: Maybe GlobalStreamOffset
  , globalStreamResultLast     :: Maybe GlobalStreamOffset
} deriving Show

newtype PostEventResult = PostEventResult (Either EventStoreActionError EventWriteResult) deriving Show
newtype ReadStreamResult = ReadStreamResult (Either EventStoreActionError (Maybe StreamResult)) deriving Show
newtype ReadAllResult = ReadAllResult (Either EventStoreActionError GlobalStreamResult) deriving Show
newtype ReadEventResult = ReadEventResult (Either EventStoreActionError (Maybe RecordedEvent)) deriving Show

data PostEventRequest = PostEventRequest {
   perStreamId        :: Text,
   perExpectedVersion :: Maybe Int64,
   perEvents          :: NonEmpty EventEntry
} deriving (Show)

instance QC.Arbitrary PostEventRequest where
  arbitrary = PostEventRequest <$> (fromString <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> ((:|) <$> QC.arbitrary <*> QC.arbitrary)

data FeedDirection = FeedDirectionForward | FeedDirectionBackward
  deriving (Eq, Show)

instance QC.Arbitrary FeedDirection where
  arbitrary = QC.elements [FeedDirectionForward, FeedDirectionBackward]

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: StreamId,
   rsrStartEventNumber :: Maybe Int64,
   rsrMaxItems         :: Natural,
   rsrDirection        :: FeedDirection
} deriving (Show)

data ReadEventRequest = ReadEventRequest {
   rerStreamId    :: Text,
   rerEventNumber :: Int64
} deriving (Show)

data ReadAllRequest = ReadAllRequest {
      readAllRequestStartPosition :: Maybe GlobalFeedPosition
    , readAllRequestMaxItems      :: Natural
    , readAllRequestDirection     :: FeedDirection
} deriving (Show)

data EventWriteResult = WriteSuccess | WrongExpectedVersion | EventExists | WriteError deriving (Eq, Show)

ensureExpectedVersion :: DynamoCmdWithErrors q m => StreamId -> Int64 -> m Bool
ensureExpectedVersion _streamId (-1) = return True
ensureExpectedVersion _streamId 0 = return True
ensureExpectedVersion streamId expectedEventNumber =
  checkEventNumber <$> getLastStreamItem streamId
  where
    checkEventNumber Nothing = False
    checkEventNumber (Just StreamEntry {..}) =
      let lastEventNumber = streamEntryFirstEventNumber + fromIntegral (length streamEntryEventEntries) - 1
      in lastEventNumber == expectedEventNumber

postEventRequestProgram :: (DynamoCmdWithErrors q m) => PostEventRequest -> m EventWriteResult
postEventRequestProgram (PostEventRequest sId ev eventEntries) = do
  let eventId = (eventEntryEventId . NonEmpty.head) eventEntries
  dynamoKeyOrError <- getDynamoKey sId ev eventId
  case dynamoKeyOrError of Left a -> return a
                           Right dynamoKey -> writeMyEvent dynamoKey
  where
    writeMyEvent :: (DynamoCmdWithErrors q m) => Int64 -> m EventWriteResult
    writeMyEvent eventNumber = do
      let streamEntry = StreamEntry {
            streamEntryStreamId = StreamId sId,
            streamEntryFirstEventNumber = eventNumber,
            streamEntryEventEntries = eventEntries,
            streamEntryNeedsPaging = True }
      writeResult <- writeStreamItem streamEntry
      return $ toEventResult writeResult
    getDynamoKey :: (DynamoCmdWithErrors q m) => Text -> Maybe Int64 -> EventId -> m (Either EventWriteResult Int64)
    getDynamoKey streamId Nothing eventId = do
      lastEvent <- P.head $ recordedEventProducerBackward (StreamId streamId) Nothing 1
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

toRecordedEvent :: (DynamoCmdWithErrors q m) => StreamEntry -> m (NonEmpty RecordedEvent)
toRecordedEvent StreamEntry{..} = do
  let eventEntriesWithEventNumber = NonEmpty.zip (streamEntryFirstEventNumber :| [streamEntryFirstEventNumber + 1 ..]) streamEntryEventEntries
  let (StreamId streamId) = streamEntryStreamId
  let buildEvent (eventNumber, EventEntry{..}) = RecordedEvent streamId eventNumber (BL.toStrict eventEntryData) (eventTypeToText eventEntryType) (unEventTime eventEntryCreated) eventEntryEventId eventEntryIsJson
  let recordedEvents = buildEvent <$> eventEntriesWithEventNumber
  return recordedEvents

toRecordedEventBackward :: (DynamoCmdWithErrors q m) => StreamEntry -> m (NonEmpty RecordedEvent)
toRecordedEventBackward readResult = NonEmpty.reverse <$> toRecordedEvent readResult

streamItemToRecordedEventBackwardPipe :: (DynamoCmdWithErrors q m) => Pipe StreamEntry RecordedEvent m ()
streamItemToRecordedEventBackwardPipe = forever $ do
  streamItem <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEventBackward streamItem
  forM_ (NonEmpty.toList recordedEvents) yield

streamItemToRecordedEventPipe :: (DynamoCmdWithErrors q m) => Pipe StreamEntry RecordedEvent m ()
streamItemToRecordedEventPipe = forever $ do
  streamItem <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEvent streamItem
  forM_ (NonEmpty.toList recordedEvents) yield

recordedEventProducerBackward :: (DynamoCmdWithErrors q m) => StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent m ()
recordedEventProducerBackward streamId lastEvent batchSize =
  let
    maxEventToRetrieve = (+1) <$> lastEvent
  in
    streamEntryProducer QueryDirectionBackward streamId maxEventToRetrieve batchSize
    >-> streamItemToRecordedEventBackwardPipe
    >-> filterLastEvent lastEvent
  where
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber)

recordedEventProducerForward :: (DynamoCmdWithErrors q m) => StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent m ()
recordedEventProducerForward streamId Nothing batchSize =
  streamEntryProducer QueryDirectionForward streamId Nothing batchSize >-> streamItemToRecordedEventPipe
recordedEventProducerForward streamId firstEvent batchSize =
  (streamEntryProducer QueryDirectionBackward streamId ((+1) <$> firstEvent) 1 >-> streamItemToRecordedEventPipe -- first page backward
     >>
     streamEntryProducer QueryDirectionForward streamId firstEvent batchSize >-> streamItemToRecordedEventPipe) -- rest of the pages
    >->
    filterFirstEvent firstEvent
  where
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber)

getReadEventRequestProgram :: (DynamoCmdWithErrors q m) => ReadEventRequest -> m (Maybe RecordedEvent)
getReadEventRequestProgram (ReadEventRequest sId eventNumber) =
  P.head $
    recordedEventProducerBackward (StreamId sId) (Just eventNumber) 1
    >-> P.dropWhile ((/= eventNumber) . recordedEventNumber)

buildStreamResult :: FeedDirection -> Maybe Int64 -> [RecordedEvent] -> Maybe Int64 -> Natural -> Maybe StreamResult
buildStreamResult _ Nothing _ _ _ = Nothing
buildStreamResult FeedDirectionBackward (Just lastEvent) events requestedStartEventNumber maxItems =
  let
    maxEventNumber = maximum $ recordedEventNumber <$> events
    startEventNumber = fromMaybe maxEventNumber requestedStartEventNumber
    nextEventNumber = startEventNumber - fromIntegral maxItems
  in Just StreamResult {
    streamResultEvents = events,
    streamResultFirst = Just (FeedDirectionBackward, EventStartHead, maxItems),
    streamResultNext =
      if nextEventNumber >= 0 then
        Just (FeedDirectionBackward, EventStartPosition nextEventNumber, maxItems)
      else Nothing,
    streamResultPrevious = Just (FeedDirectionForward, EventStartPosition (min (startEventNumber + 1) (lastEvent + 1)), maxItems),
    streamResultLast =
      if nextEventNumber >= 0 then
        Just (FeedDirectionForward, EventStartPosition 0, maxItems)
      else Nothing
  }
buildStreamResult FeedDirectionForward (Just _lastEvent) events requestedStartEventNumber maxItems =
  let
    maxEventNumber = maximumMay $ recordedEventNumber <$> events
    minEventNumber = minimumMay $ recordedEventNumber <$> events
    nextEventNumber = fromMaybe (fromMaybe 0 ((\x -> x - 1) <$> requestedStartEventNumber)) ((\x -> x - 1) <$> minEventNumber)
    previousEventNumber = (+1) <$> maxEventNumber
  in Just StreamResult {
    streamResultEvents = events,
    streamResultFirst = Just (FeedDirectionBackward, EventStartHead, maxItems),
    streamResultNext =
        if nextEventNumber >= 0 then
        Just (FeedDirectionBackward, EventStartPosition nextEventNumber, maxItems)
      else Nothing,
    streamResultPrevious = (\eventNumber -> (FeedDirectionForward, EventStartPosition eventNumber, maxItems)) <$> previousEventNumber,
    streamResultLast =
      if maybe True (> 0) minEventNumber then
        Just (FeedDirectionForward, EventStartPosition 0, maxItems)
      else Nothing
  }

getLastEvent :: (DynamoCmdWithErrors q m) => StreamId -> m (Maybe Int64)
getLastEvent streamId = do
  x <- P.head $ recordedEventProducerBackward streamId Nothing 1
  return $ recordedEventNumber <$> x

getReadStreamRequestProgram :: (DynamoCmdWithErrors q m) => ReadStreamRequest -> m (Maybe StreamResult)
getReadStreamRequestProgram (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionBackward) =
  do
    lastEvent <- getLastEvent streamId
    events <-
      P.toListM $
        recordedEventProducerBackward streamId startEventNumber 10
          >-> filterLastEvent startEventNumber
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionBackward lastEvent events startEventNumber maxItems
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r > minimumEventNumber v)
    minimumEventNumber start = fromIntegral start - fromIntegral maxItems
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber)
getReadStreamRequestProgram (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionForward) =
  do
    lastEvent <- getLastEvent streamId
    events <-
      P.toListM $
        recordedEventProducerForward streamId startEventNumber 10
          >-> filterFirstEvent startEventNumber
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionForward lastEvent events startEventNumber maxItems
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r <= maximumEventNumber v)
    maximumEventNumber start = fromIntegral start + fromIntegral maxItems - 1
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber)

getPagesBackward :: DynamoCmdWithErrors q m => PageKey -> Producer [(GlobalFeedPosition,EventKey)] m ()
getPagesBackward (PageKey (-1)) = return ()
getPagesBackward page = do
  result <- lift $ readPage page
  _ <- case result of (Just entries) -> yield (globalFeedItemToEventKeys entries)
                      Nothing        -> lift $ throwError (EventStoreActionErrorInvalidGlobalFeedPage page)
  getPagesBackward (page - 1)

feedEntryToEventKeys :: FeedEntry -> [EventKey]
feedEntryToEventKeys FeedEntry { feedEntryStream = streamId, feedEntryNumber = eventNumber, feedEntryCount = entryCount } =
  (\number -> EventKey(streamId, number)) <$> take entryCount [eventNumber..]

globalFeedItemToEventKeys :: GlobalFeedItem -> [(GlobalFeedPosition, EventKey)]
globalFeedItemToEventKeys GlobalFeedItem{..} =
  let eventKeys = join $ feedEntryToEventKeys <$> toList globalFeedItemFeedEntries
  in zip (GlobalFeedPosition globalFeedItemPageKey <$> [0..]) eventKeys

getPageItemsBackward :: DynamoCmdWithErrors q m => PageKey -> Producer (GlobalFeedPosition, EventKey) m ()
getPageItemsBackward startPage =
  getPagesBackward startPage >-> readResultToEventKeys
  where
    readResultToEventKeys = forever $
      (reverse <$> await) >>= mapM_ yield

getFirstPageBackward :: DynamoCmdWithErrors q m => GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ()
getFirstPageBackward position@GlobalFeedPosition{..} = do
  items <- lift $ readPage globalFeedPositionPage
  let itemsBeforePosition = (globalFeedItemToEventKeys <$> items) >>= takeExactMay (globalFeedPositionOffset + 1)
  maybe notFoundError yieldItemsInReverse itemsBeforePosition
  where
    notFoundError = lift $ throwError (EventStoreActionErrorInvalidGlobalFeedPosition position)
    yieldItemsInReverse = mapM_ yield . reverse

getGlobalFeedBackward :: DynamoCmdWithErrors q m => Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ()
getGlobalFeedBackward Nothing = do
  lastKnownPage <- lift HeadItem.getLastFullPage
  let lastKnownPage' = fromMaybe (PageKey 0) lastKnownPage
  lastItem <- lift $ P.last (getPageItemsForward lastKnownPage')
  let lastPosition = fst <$> lastItem
  maybe (return ()) (getGlobalFeedBackward . Just) lastPosition

getGlobalFeedBackward (Just (position@GlobalFeedPosition{..})) =
  getFirstPageBackward position >> getPageItemsBackward (globalFeedPositionPage - 1)

getPagesForward :: (DynamoCmdWithErrors q m) => PageKey -> Producer [(GlobalFeedPosition,EventKey)] m ()
getPagesForward startPage = do
  result <- lift $ readPage startPage
  case result of (Just entries) -> yield (globalFeedItemToEventKeys entries) >> getPagesForward (startPage + 1)
                 Nothing        -> return ()

getPageItemsForward :: (DynamoCmdWithErrors q m) => PageKey -> Producer (GlobalFeedPosition, EventKey) m ()
getPageItemsForward startPage =
  getPagesForward startPage >-> readResultToEventKeys
  where
    readResultToEventKeys = forever $
      await >>= mapM_ yield

lookupEvent :: DynamoCmdWithErrors q m => StreamId -> Int64 -> m (Maybe RecordedEvent)
lookupEvent streamId eventNumber =
  P.head $
    (recordedEventProducerBackward streamId (Just eventNumber) 1)
    >->
    P.dropWhile ((/= eventNumber). recordedEventNumber)

lookupEventKey :: DynamoCmdWithErrors q m => Pipe (GlobalFeedPosition, EventKey) (GlobalFeedPosition, RecordedEvent) m ()
lookupEventKey = forever $ do
  (position, eventKey@(EventKey(streamId, eventNumber))) <- await
  (maybeRecordedEvent :: Maybe RecordedEvent) <- lift $ lookupEvent streamId eventNumber
  let withPosition = (\e -> (position, e)) <$> maybeRecordedEvent
  maybe (throwError $ EventStoreActionErrorCouldNotFindEvent eventKey) yield withPosition

getReadAllRequestProgram :: DynamoCmdWithErrors q m => ReadAllRequest -> m GlobalStreamResult
getReadAllRequestProgram ReadAllRequest
  {
    readAllRequestDirection = FeedDirectionForward
  , readAllRequestStartPosition = readAllRequestStartPosition
  , readAllRequestMaxItems = readAllRequestMaxItems
  } = do
  events <- P.toListM $
    getPageItemsForward 0
    >-> lookupEventKey
    >-> filterFirstEvent readAllRequestStartPosition
    >-> P.take (fromIntegral readAllRequestMaxItems)
  let previousEventPosition = fst <$> lastMay events
  nextEvent <- case readAllRequestStartPosition of Nothing -> return Nothing
                                                   Just startPosition -> do
                                                     nextEvents <- P.toListM $
                                                      getGlobalFeedBackward (Just startPosition)
                                                      >-> P.map fst
                                                      >-> P.filter (<= startPosition)
                                                      >-> P.take 1
                                                     return $ listToMaybe nextEvents
  return GlobalStreamResult {
    globalStreamResultEvents = snd <$> events,
    globalStreamResultNext = (\pos -> (FeedDirectionBackward, GlobalStartPosition pos, readAllRequestMaxItems)) <$> nextEvent,
    globalStreamResultPrevious = (\pos -> (FeedDirectionForward, GlobalStartPosition pos, readAllRequestMaxItems)) <$> previousEventPosition,
    globalStreamResultFirst = Just (FeedDirectionBackward, GlobalStartHead, readAllRequestMaxItems),
    globalStreamResultLast = const (FeedDirectionForward, GlobalStartHead, readAllRequestMaxItems) <$> nextEvent -- only show last if there is a next
  }
  where
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just startPosition) = P.filter ((> startPosition) . fst)
getReadAllRequestProgram ReadAllRequest
  {
    readAllRequestDirection = FeedDirectionBackward
  , readAllRequestStartPosition = readAllRequestStartPosition
  , readAllRequestMaxItems = readAllRequestMaxItems
  } = do
  let maxItems = fromIntegral readAllRequestMaxItems
  eventsPlus1 <- P.toListM $
    getGlobalFeedBackward readAllRequestStartPosition
    >-> lookupEventKey
    >-> filterLastEvent readAllRequestStartPosition
    >-> P.take (maxItems + 1)
  let events = snd <$> take maxItems eventsPlus1
  let previousEventPosition = fst <$> headMay eventsPlus1
  let nextEventBackwardPosition = fst <$> listToMaybe (drop maxItems eventsPlus1)
  return GlobalStreamResult {
    globalStreamResultEvents = events,
    globalStreamResultNext = (\pos -> (FeedDirectionBackward, GlobalStartPosition pos, readAllRequestMaxItems)) <$> nextEventBackwardPosition,
    globalStreamResultPrevious = (\pos -> (FeedDirectionForward, GlobalStartPosition pos, readAllRequestMaxItems)) <$> previousEventPosition,
    globalStreamResultFirst = Just (FeedDirectionBackward, GlobalStartHead, readAllRequestMaxItems),
    globalStreamResultLast = const (FeedDirectionForward, GlobalStartHead, readAllRequestMaxItems) <$> nextEventBackwardPosition -- only show last if there is a next
  }
  where
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just startPosition) = P.filter ((<= startPosition) . fst)