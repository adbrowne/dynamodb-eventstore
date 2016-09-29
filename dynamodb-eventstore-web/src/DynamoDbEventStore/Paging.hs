{-# LANGUAGE OverloadedStrings #-}
module DynamoDbEventStore.Paging
  (runStreamRequest
  ,FeedDirection(..)
  ,StreamResult(..)
  ,EventStartPosition(..)
  ,ReadStreamRequest(..)
  ,StreamOffset)
  where

import DynamoDbEventStore.ProjectPrelude
import qualified Test.QuickCheck                       as QC
import qualified Pipes.Prelude                         as P
import Safe
import DynamoDbEventStore.Types (RecordedEvent(..), StreamId, QueryDirection(..))

data FeedDirection = FeedDirectionForward | FeedDirectionBackward
  deriving (Eq, Show)

instance QC.Arbitrary FeedDirection where
  arbitrary = QC.elements [FeedDirectionForward, FeedDirectionBackward]

data EventStartPosition = EventStartHead | EventStartPosition Int64 deriving (Show, Eq)

type StreamOffset = (FeedDirection, EventStartPosition, Natural)

data StreamResult = StreamResult {
    streamResultEvents   :: [RecordedEvent]
  , streamResultFirst    :: Maybe StreamOffset
  , streamResultNext     :: Maybe StreamOffset
  , streamResultPrevious :: Maybe StreamOffset
  , streamResultLast     :: Maybe StreamOffset
} deriving Show

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: StreamId,
   rsrStartEventNumber :: Maybe Int64,
   rsrMaxItems         :: Natural,
   rsrDirection        :: FeedDirection
} deriving (Show)

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

getLastEvent
  :: (Monad m)
  => (QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent m ())
  -> StreamId
  -> m (Maybe Int64)
getLastEvent eventProducer streamId = do
  x <- P.head $ eventProducer QueryDirectionBackward streamId Nothing 1
  return $ recordedEventNumber <$> x

runStreamRequest
  :: (Monad m)
  => (QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent m ())
  -> ReadStreamRequest
  -> m (Maybe StreamResult)
runStreamRequest eventProducer (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionBackward) =
  do
    lastEvent <- getLastEvent eventProducer streamId
    events <-
      P.toListM $
          eventProducer QueryDirectionBackward streamId startEventNumber 10
          >-> filterLastEvent startEventNumber
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionBackward lastEvent events startEventNumber maxItems
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r > minimumEventNumber v)
    minimumEventNumber start = fromIntegral start - fromIntegral maxItems
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber)
runStreamRequest eventProducer (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionForward) =
  do
    lastEvent <- getLastEvent eventProducer streamId
    events <-
      P.toListM $
        eventProducer QueryDirectionForward streamId startEventNumber 10
          >-> filterFirstEvent startEventNumber
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionForward lastEvent events startEventNumber maxItems
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r <= maximumEventNumber v)
    maximumEventNumber start = fromIntegral start + fromIntegral maxItems - 1
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber)
