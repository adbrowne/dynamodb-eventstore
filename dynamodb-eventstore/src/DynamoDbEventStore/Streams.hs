{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
module DynamoDbEventStore.Streams
  (streamEventsProducer)
  where

import DynamoDbEventStore.ProjectPrelude
import qualified Pipes.Prelude                         as P
import qualified Data.List.NonEmpty                    as NonEmpty
import qualified Data.ByteString.Lazy                  as BL

import DynamoDbEventStore.EventStoreCommands (MonadEsDsl)
import DynamoDbEventStore.Types (QueryDirection(..),EventStoreActionError,StreamId(..),RecordedEvent(..))
import           DynamoDbEventStore.Storage.StreamItem (StreamEntry(..), EventEntry(..),eventTypeToText, unEventTime,streamEntryProducer)

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
