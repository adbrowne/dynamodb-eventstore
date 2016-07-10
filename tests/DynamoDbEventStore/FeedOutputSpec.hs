{-# LANGUAGE OverloadedStrings #-}

module DynamoDbEventStore.FeedOutputSpec (tests) where

import           BasicPrelude
import           Data.Maybe                            (fromJust)
import           Data.Time.Format
import qualified Data.UUID                             as UUID
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.Feed
import           Test.Tasty
import           Test.Tasty.HUnit
import           Text.Blaze.Renderer.String

-- a set of tests that will detect unexpected changes in converting feeds/entries to
-- or from their feed types
-- ideally unit tests should catch these errors

globalFeedXmlOutputCheck :: Assertion
globalFeedXmlOutputCheck =
  let
    baseUri = "http://localhost:2113"
    streamId = StreamId "%24all"
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed = globalStreamResultsToFeed baseUri streamId sampleTime globalFeedResult
    globalFeedResult = GlobalStreamResult {
      globalStreamResultEvents = [
        RecordedEvent{
            recordedEventStreamId = "MyStream"
            , recordedEventNumber = 2
            , recordedEventData = fromString "{ \"a\": 2 }"
            , recordedEventType = "Event Type"
            , recordedEventCreated = sampleTime
            , recordedEventId = EventId (fromJust $ UUID.fromString "1449f441-e249-4381-92b8-a3e2a444c95c")
            , recordedEventIsJson = True
                    }]
      , globalStreamResultFirst = Just (FeedDirectionForward, GlobalStartHead, 1)
      , globalStreamResultLast = Nothing
      , globalStreamResultNext = Nothing
      , globalStreamResultPrevious = Nothing}

    expectedXml = "<feed xmlns=\"http://www.w3.org/2005/Atom\"><title>All events</title><id>http://localhost:2113/streams/%24all</id><updated>2016-05-08T12:49:41.000000Z</updated><author><name>EventStore</name></author><link href=\"http://localhost:2113/streams/%24all\" rel=\"self\" /><link href=\"http://localhost:2113/streams/%24all/head/forward/1\" rel=\"first\" /><entry><title>2@MyStream</title><id>http://localhost:2113/streams/MyStream/2</id><updated>2016-05-08T12:49:41.000000Z</updated><author><name>EventStore</name></author><summary>Event Type</summary><link href=\"http://localhost:2113/streams/MyStream/2\" rel=\"edit\" /><link href=\"http://localhost:2113/streams/MyStream/2\" rel=\"alternate\" /></entry></feed>"
  in assertEqual "feed is equal to expected xml" expectedXml (renderMarkup $ xmlFeed buildFeed)

streamFeedXmlOutputCheck :: Assertion
streamFeedXmlOutputCheck =
  let
    baseUri = "http://localhost:2113"
    streamId = StreamId "MyStream"
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed = streamResultsToFeed baseUri streamId sampleTime streamResult
    streamResult = StreamResult {
      streamResultEvents = [
        RecordedEvent{
            recordedEventStreamId = "MyStream"
            , recordedEventNumber = 2
            , recordedEventData = fromString "{ \"a\": 2 }"
            , recordedEventType = "Event Type"
            , recordedEventCreated = sampleTime
            , recordedEventId = EventId (fromJust $ UUID.fromString "1449f441-e249-4381-92b8-a3e2a444c95c")
            , recordedEventIsJson = True
                    }]
      , streamResultFirst = Just (FeedDirectionForward, EventStartHead, 1)
      , streamResultLast = Nothing
      , streamResultNext = Nothing
      , streamResultPrevious = Nothing}

    expectedXml = "<feed xmlns=\"http://www.w3.org/2005/Atom\"><title>Event stream 'MyStream'</title><id>http://localhost:2113/streams/MyStream</id><updated>2016-05-08T12:49:41.000000Z</updated><author><name>EventStore</name></author><link href=\"http://localhost:2113/streams/MyStream\" rel=\"self\" /><link href=\"http://localhost:2113/streams/MyStream/head/forward/1\" rel=\"first\" /><entry><title>2@MyStream</title><id>http://localhost:2113/streams/MyStream/2</id><updated>2016-05-08T12:49:41.000000Z</updated><author><name>EventStore</name></author><summary>Event Type</summary><link href=\"http://localhost:2113/streams/MyStream/2\" rel=\"edit\" /><link href=\"http://localhost:2113/streams/MyStream/2\" rel=\"alternate\" /></entry></feed>"
  in assertEqual "feed is equal to expected xml" expectedXml (renderMarkup $ xmlFeed buildFeed)

tests :: [TestTree]
tests = [
  testCase "Global Feed XML output check" globalFeedXmlOutputCheck
  , testCase "Stream Feed XML output check" streamFeedXmlOutputCheck]
