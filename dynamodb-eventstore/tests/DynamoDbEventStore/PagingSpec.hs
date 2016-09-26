{-# LANGUAGE OverloadedStrings #-}
module DynamoDbEventStore.PagingSpec
  (tests)
  where

import DynamoDbEventStore.ProjectPrelude
import Data.Maybe (fromJust)
import DynamoDbEventStore.Paging
import DynamoDbEventStore.Types (EventStoreActionError,RecordedEvent(..),EventId,StreamId(..),EventId(..),QueryDirection(..))
import Safe
import Test.Tasty
import qualified Data.Text.Encoding as T
import Test.Tasty.HUnit
import Data.Time
import qualified Data.UUID as UUID

testStreamId :: Text
testStreamId = "MyStream"

sampleTime :: UTCTime
sampleTime =
    parseTimeOrError
        True
        defaultTimeLocale
        rfc822DateFormat
        "Sun, 08 May 2016 12:49:41 +0000"

eventIdFromString :: String -> EventId
eventIdFromString = EventId . fromJust . UUID.fromString

sampleEventId :: EventId
sampleEventId = eventIdFromString "c2cc10e1-57d6-4b6f-9899-38d972112d8c"

sampleItems :: Monad m => Int64 -> QueryDirection -> Maybe Int64 -> Producer RecordedEvent m ()
sampleItems count direction startEvent =
    let
      maxEventNumber = count - 1
      eventNumbers QueryDirectionForward Nothing = [0..maxEventNumber]
      eventNumbers QueryDirectionForward (Just s) = [s..maxEventNumber]
      eventNumbers QueryDirectionBackward Nothing = [maxEventNumber,(maxEventNumber-1)..0]
      eventNumbers QueryDirectionBackward (Just s) = [s,(s - 1)..0]
    in traverse_ (yield . buildRecordedEvent) (eventNumbers direction startEvent)
    where buildRecordedEvent index =
            RecordedEvent {
                recordedEventStreamId = testStreamId
                , recordedEventNumber = index
                , recordedEventType = tshow index
                , recordedEventData = T.encodeUtf8 "Some data"
                , recordedEventCreated = sampleTime
                , recordedEventId = sampleEventId
                , recordedEventIsJson = False }

streamEventsProducer :: (Monad m) => QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent m ()
streamEventsProducer direction _streamId startEvent batchSize =
  sampleItems 29 direction startEvent

getSampleItems
    :: Maybe Int64
    -> Natural
    -> FeedDirection
    -> Either EventStoreActionError (Maybe StreamResult)
getSampleItems startEvent maxItems direction =
  let request = ReadStreamRequest {
        rsrStreamId = StreamId testStreamId,
        rsrStartEventNumber = startEvent,
        rsrMaxItems = maxItems,
        rsrDirection = direction
                }
  in runStreamRequest streamEventsProducer request

streamLinkTests :: [TestTree]
streamLinkTests = 
    let endOfFeedBackward = 
            ( "End of feed backward"
            , getSampleItems Nothing 20 FeedDirectionBackward)
        middleOfFeedBackward = 
            ( "Middle of feed backward"
            , getSampleItems (Just 26) 20 FeedDirectionBackward)
        startOfFeedBackward = 
            ( "Start of feed backward"
            , getSampleItems (Just 1) 20 FeedDirectionBackward)
        pastEndOfFeedBackward = 
            ( "Past end of feed backward"
            , getSampleItems (Just 100) 20 FeedDirectionBackward)
        startOfFeedForward = 
            ( "Start of feed forward"
            , getSampleItems Nothing 20 FeedDirectionForward)
        middleOfFeedForward = 
            ( "Middle of feed forward"
            , getSampleItems (Just 3) 20 FeedDirectionForward)
        endOfFeedForward = 
            ( "End of feed forward"
            , getSampleItems (Just 20) 20 FeedDirectionForward)
        pastEndOfFeedForward = 
            ( "Past end of feed forward"
            , getSampleItems (Just 100) 20 FeedDirectionForward)
        streamResultLast' = ("last", streamResultLast)
        streamResultFirst' = ("first", streamResultFirst)
        streamResultNext' = ("next", streamResultNext)
        streamResultPrevious' = ("previous", streamResultPrevious)
        linkAssert (feedResultName,feedResult) (linkName,streamLink) expectedResult = 
            testCase
                ("Unit - " <> feedResultName <> " - " <> linkName <> " link") $
            assertEqual
                ("Should have " <> linkName <> " link")
                (Right (Just expectedResult))
                (fmap2 streamLink feedResult)
    in [ linkAssert
             endOfFeedBackward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert
             endOfFeedBackward
             streamResultLast'
             (Just (FeedDirectionForward, EventStartPosition 0, 20))
       , linkAssert
             endOfFeedBackward
             streamResultNext'
             (Just (FeedDirectionBackward, EventStartPosition 8, 20))
       , linkAssert
             endOfFeedBackward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 29, 20))
       , linkAssert
             middleOfFeedBackward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert
             middleOfFeedBackward
             streamResultLast'
             (Just (FeedDirectionForward, EventStartPosition 0, 20))
       , linkAssert
             middleOfFeedBackward
             streamResultNext'
             (Just (FeedDirectionBackward, EventStartPosition 6, 20))
       , linkAssert
             middleOfFeedBackward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 27, 20))
       , linkAssert
             startOfFeedBackward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert startOfFeedBackward streamResultLast' Nothing
       , linkAssert startOfFeedBackward streamResultNext' Nothing
       , linkAssert
             startOfFeedBackward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 2, 20))
       , linkAssert
             pastEndOfFeedBackward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert
             pastEndOfFeedBackward
             streamResultLast'
             (Just (FeedDirectionForward, EventStartPosition 0, 20))
       , linkAssert
             pastEndOfFeedBackward
             streamResultNext'
             (Just (FeedDirectionBackward, EventStartPosition 80, 20))
       , linkAssert
             pastEndOfFeedBackward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 29, 20))
       , linkAssert
             startOfFeedForward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert startOfFeedForward streamResultLast' Nothing
       , linkAssert startOfFeedForward streamResultNext' Nothing
       , linkAssert
             startOfFeedForward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 20, 20))
       , linkAssert
             middleOfFeedForward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert
             middleOfFeedForward
             streamResultLast'
             (Just (FeedDirectionForward, EventStartPosition 0, 20))
       , linkAssert
             middleOfFeedForward
             streamResultNext'
             (Just (FeedDirectionBackward, EventStartPosition 2, 20))
       , linkAssert
             middleOfFeedForward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 23, 20))
       , linkAssert
             endOfFeedForward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert
             endOfFeedForward
             streamResultLast'
             (Just (FeedDirectionForward, EventStartPosition 0, 20))
       , linkAssert
             endOfFeedForward
             streamResultNext'
             (Just (FeedDirectionBackward, EventStartPosition 19, 20))
       , linkAssert
             endOfFeedForward
             streamResultPrevious'
             (Just (FeedDirectionForward, EventStartPosition 29, 20))
       , linkAssert
             pastEndOfFeedForward
             streamResultFirst'
             (Just (FeedDirectionBackward, EventStartHead, 20))
       , linkAssert
             pastEndOfFeedForward
             streamResultLast'
             (Just (FeedDirectionForward, EventStartPosition 0, 20))
       , linkAssert
             pastEndOfFeedForward
             streamResultNext'
             (Just (FeedDirectionBackward, EventStartPosition 99, 20))
       , linkAssert pastEndOfFeedForward streamResultPrevious' Nothing]

tests :: [TestTree]
tests = [
    testGroup "Single Stream Link Tests" streamLinkTests
    ]
