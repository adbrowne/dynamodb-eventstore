{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module DynamoDbEventStore.GlobalPagingSpec
  (tests)
  where

import DynamoDbEventStore.ProjectPrelude
import DynamoDbEventStore.Paging (FeedDirection(..))
import DynamoDbEventStore.GlobalPaging
import DynamoDbEventStore.PagingSpec(buildRecordedEvent)
import DynamoDbEventStore.Types (
  PageKey(..),
  GlobalFeedPosition(..),
  StreamId(..),
  EventStoreActionError(..),
  QueryDirection(..),
  EventKey(..),
  RecordedEvent(..))
import Test.Tasty
import Test.Tasty.HUnit
import Control.Lens
import qualified Pipes.Prelude as P

fibs :: [Int]
fibs = 
    let acc (a,b) = Just (a + b, (b, a + b))
    in 1 : 1 : unfoldr acc (1, 1)

groupByFibs :: [a] -> [[a]]
groupByFibs as = 
    let acc (_,[]) = Nothing
        acc ([],_) = error "ran out of fibs that should not happen"
        acc (x:xs,ys) = Just (take x ys, (xs, drop x ys))
    in unfoldr acc (fibs, as)

sampleItems :: Int64 -> [(GlobalFeedPosition, RecordedEvent)]
sampleItems count =
  let
    items = buildRecordedEvent <$> [0..(count - 1)]
    pages = zip [0 ..] (groupByFibs items)
  in
    join $ buildPositions <$> pages
  where
    buildPositions :: (Int64, [RecordedEvent]) -> [(GlobalFeedPosition, RecordedEvent)]
    buildPositions (page, xs) =
      (over _1 (\i -> GlobalFeedPosition (PageKey page) i))
      <$>
      zip [0 ..] xs

sampleGlobalFeedItems :: Monad m => Int64 -> QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, RecordedEvent) m ()
sampleGlobalFeedItems count direction startPosition =
  let
    events QueryDirectionForward Nothing = sampleItems count
    events QueryDirectionForward (Just s) =
      sampleItems count
      & dropWhile (views _1 (< s))
      & tail
    events QueryDirectionBackward Nothing = reverse $ sampleItems count
    events QueryDirectionBackward (Just s) =
      sampleItems count
      & reverse
      & dropWhile (views _1 (> s))
  in
    traverse_ yield $ events direction startPosition

globalEventsProducer :: Monad m => QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, RecordedEvent) m ()
globalEventsProducer = sampleGlobalFeedItems 29

globalEventKeysProducer :: Monad m => QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) m ()
globalEventKeysProducer direction startPosition =
  sampleGlobalFeedItems 29 direction startPosition
  >->
  (P.map (over _2 recordedEventToEventKey))
  where
    recordedEventToEventKey RecordedEvent {..} =
      EventKey (StreamId recordedEventStreamId, recordedEventNumber)

getSampleGlobalItems
    :: Maybe GlobalFeedPosition
    -> Natural
    -> FeedDirection
    -> Either EventStoreActionError GlobalStreamResult
getSampleGlobalItems startPosition maxItems direction = 
    let readAllRequest = ReadAllRequest startPosition maxItems direction
    in runGlobalStreamRequest globalEventsProducer globalEventKeysProducer readAllRequest

{-
globalStreamPages:
0: 0 (1)
1: 1 (1)
2: 2,3 (2)
3: 4,5,6 (3)
4: 7,8,9,10,11 (5)
5: 12,13,14,15,16,17,18,19 (8)
6: 20,21,22,23,24,25,26,27,28,29,30,31,32 (13)
7: 33..53 (21)
8: 54..87 (34)
9: 88,89,90,91,92,93,94,95,96,97,98,99,100.. (55)
-}
globalStreamPagingTests
    :: [TestTree]
globalStreamPagingTests = 
    let getEventTypes start maxItems direction = 
            fmap2 recordedEventType $
            globalStreamResultEvents <$>
            getSampleGlobalItems start maxItems direction
        resultAssert testName start maxItems direction expectedBodies = 
            testCase testName $
            assertEqual
                "Should return events"
                (Right expectedBodies)
                (getEventTypes start maxItems direction)
    in [ resultAssert
             "Start of feed forward - start = Nothing"
             Nothing
             1
             FeedDirectionForward
             ["0"]
       , resultAssert
             "0 0 of feed forward"
             (Just $ GlobalFeedPosition 0 0)
             1
             FeedDirectionForward
             ["1"]
       , resultAssert
             "Middle of the feed forward"
             (Just $ GlobalFeedPosition 1 0)
             3
             FeedDirectionForward
             ["2", "3", "4"]
       , resultAssert
             "End of the feed forward"
             (Just $ GlobalFeedPosition 6 7)
             3
             FeedDirectionForward
             ["28"]
       , resultAssert
             "End of feed backward - start = Nothing"
             Nothing
             3
             FeedDirectionBackward
             ["28", "27", "26"]
       , resultAssert
             "End of the feed backward"
             (Just $ GlobalFeedPosition 6 8)
             3
             FeedDirectionBackward
             ["28", "27", "26"]
       , resultAssert
             "Middle of the feed backward"
             (Just $ GlobalFeedPosition 5 7)
             3
             FeedDirectionBackward
             ["19", "18", "17"]
       , resultAssert
             "End of feed backward"
             (Just $ GlobalFeedPosition 0 0)
             1
             FeedDirectionBackward
             ["0"]]

tests :: [TestTree]
tests = [
    testGroup "Global Stream Paging Tests" globalStreamPagingTests
    ]
