{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module DynamoDbEventStore.GlobalPaging
  (GlobalStreamResult(..)
  ,GlobalStreamOffset
  ,GlobalStartPosition(..)
  ,ReadAllRequest(..)
  ,runGlobalStreamRequest)

where

import DynamoDbEventStore.ProjectPrelude
import qualified Pipes.Prelude                         as P
import Safe
import DynamoDbEventStore.Paging (FeedDirection(..))
import DynamoDbEventStore.Types (
  GlobalFeedPosition(..),
  QueryDirection(..),
  EventKey(..),
  RecordedEvent(..))

data ReadAllRequest = ReadAllRequest {
      readAllRequestStartPosition :: Maybe GlobalFeedPosition
    , readAllRequestMaxItems      :: Natural
    , readAllRequestDirection     :: FeedDirection
} deriving (Show)

data GlobalStartPosition = GlobalStartHead | GlobalStartPosition GlobalFeedPosition deriving (Show, Eq)

data GlobalStreamResult = GlobalStreamResult {
    globalStreamResultEvents   :: [RecordedEvent]
  , globalStreamResultFirst    :: Maybe GlobalStreamOffset
  , globalStreamResultNext     :: Maybe GlobalStreamOffset
  , globalStreamResultPrevious :: Maybe GlobalStreamOffset
  , globalStreamResultLast     :: Maybe GlobalStreamOffset
} deriving Show

type GlobalStreamOffset = (FeedDirection, GlobalStartPosition, Natural)

runGlobalStreamRequest
  :: Monad m
  => (QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, RecordedEvent) m ())
  -> (QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ())
  -> ReadAllRequest
  -> m GlobalStreamResult
runGlobalStreamRequest eventProducer eventKeyProducer ReadAllRequest
  {
    readAllRequestDirection = FeedDirectionForward
  , readAllRequestStartPosition = readAllRequestStartPosition
  , readAllRequestMaxItems = readAllRequestMaxItems
  } = do
  events <- P.toListM $
    eventProducer QueryDirectionForward readAllRequestStartPosition
    >-> P.take (fromIntegral readAllRequestMaxItems)
  let previousEventPosition = fst <$> lastMay events
  nextEvent <- case readAllRequestStartPosition of Nothing -> return Nothing
                                                   Just startPosition -> do
                                                     nextEvents <- P.toListM $
                                                      eventKeyProducer QueryDirectionBackward (Just startPosition)
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
runGlobalStreamRequest eventProducer _eventKeyProducer ReadAllRequest
  {
    readAllRequestDirection = FeedDirectionBackward
  , readAllRequestStartPosition = readAllRequestStartPosition
  , readAllRequestMaxItems = readAllRequestMaxItems
  } = do
  let maxItems = fromIntegral readAllRequestMaxItems
  eventsPlus1 <- P.toListM $
    eventProducer QueryDirectionBackward readAllRequestStartPosition
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
