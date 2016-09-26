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
  Streams.EventWriteResult(..),
  PostEventResult(..),
  ReadStreamResult(..),
  ReadAllResult(..),
  ReadEventResult(..),
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
  getReadAllRequestProgram) where

import           BasicPrelude
import           Data.List.NonEmpty                    (NonEmpty (..))
import           DynamoDbEventStore.Paging
import qualified DynamoDbEventStore.Streams as Streams
import           DynamoDbEventStore.Storage.StreamItem (EventEntry(..),EventType(..),EventTime(..),unEventTime)
import           DynamoDbEventStore.EventStoreCommands hiding (readField)
import           DynamoDbEventStore.GlobalFeedWriter   (DynamoCmdWithErrors)
import           DynamoDbEventStore.Types
import           GHC.Natural
import           Pipes                                 hiding (ListT, runListT)
import qualified Pipes.Prelude                         as P
import           Safe
import qualified Test.QuickCheck                       as QC
import           Test.QuickCheck.Instances             ()

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadEvent ReadEventRequest |
  ReadAll ReadAllRequest deriving (Show)

data GlobalStartPosition = GlobalStartHead | GlobalStartPosition GlobalFeedPosition deriving (Show, Eq)

type GlobalStreamOffset = (FeedDirection, GlobalStartPosition, Natural)

data GlobalStreamResult = GlobalStreamResult {
    globalStreamResultEvents   :: [RecordedEvent]
  , globalStreamResultFirst    :: Maybe GlobalStreamOffset
  , globalStreamResultNext     :: Maybe GlobalStreamOffset
  , globalStreamResultPrevious :: Maybe GlobalStreamOffset
  , globalStreamResultLast     :: Maybe GlobalStreamOffset
} deriving Show

newtype PostEventResult = PostEventResult (Either EventStoreActionError Streams.EventWriteResult) deriving Show
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

data ReadEventRequest = ReadEventRequest {
   rerStreamId    :: Text,
   rerEventNumber :: Int64
} deriving (Show)

data ReadAllRequest = ReadAllRequest {
      readAllRequestStartPosition :: Maybe GlobalFeedPosition
    , readAllRequestMaxItems      :: Natural
    , readAllRequestDirection     :: FeedDirection
} deriving (Show)

postEventRequestProgram :: (DynamoCmdWithErrors q m) => PostEventRequest -> m Streams.EventWriteResult
postEventRequestProgram (PostEventRequest sId ev eventEntries) =
  Streams.writeEvent (StreamId sId) ev eventEntries

getReadEventRequestProgram :: (DynamoCmdWithErrors q m) => ReadEventRequest -> m (Maybe RecordedEvent)
getReadEventRequestProgram (ReadEventRequest sId eventNumber) =
  Streams.readEvent (StreamId sId) eventNumber 

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
  x <- P.head $ Streams.streamEventsProducer QueryDirectionBackward streamId Nothing 1
  return $ recordedEventNumber <$> x

getReadStreamRequestProgram :: (DynamoCmdWithErrors q m) => ReadStreamRequest -> m (Maybe StreamResult)
getReadStreamRequestProgram (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionBackward) =
  do
    lastEvent <- getLastEvent streamId
    events <-
      P.toListM $
          Streams.streamEventsProducer QueryDirectionBackward streamId startEventNumber 10
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
        Streams.streamEventsProducer QueryDirectionForward streamId startEventNumber 10
          >-> filterFirstEvent startEventNumber
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionForward lastEvent events startEventNumber maxItems
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r <= maximumEventNumber v)
    maximumEventNumber start = fromIntegral start + fromIntegral maxItems - 1
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber)


getReadAllRequestProgram :: DynamoCmdWithErrors q m => ReadAllRequest -> m GlobalStreamResult
getReadAllRequestProgram ReadAllRequest
  {
    readAllRequestDirection = FeedDirectionForward
  , readAllRequestStartPosition = readAllRequestStartPosition
  , readAllRequestMaxItems = readAllRequestMaxItems
  } = do
  events <- P.toListM $
    Streams.globalEventsProducer QueryDirectionForward readAllRequestStartPosition
    >-> P.take (fromIntegral readAllRequestMaxItems)
  let previousEventPosition = fst <$> lastMay events
  nextEvent <- case readAllRequestStartPosition of Nothing -> return Nothing
                                                   Just startPosition -> do
                                                     nextEvents <- P.toListM $
                                                      Streams.globalEventKeysProducer QueryDirectionBackward (Just startPosition)
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
getReadAllRequestProgram ReadAllRequest
  {
    readAllRequestDirection = FeedDirectionBackward
  , readAllRequestStartPosition = readAllRequestStartPosition
  , readAllRequestMaxItems = readAllRequestMaxItems
  } = do
  let maxItems = fromIntegral readAllRequestMaxItems
  eventsPlus1 <- P.toListM $
    Streams.globalEventsProducer QueryDirectionBackward readAllRequestStartPosition
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
