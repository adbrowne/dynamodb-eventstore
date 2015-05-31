{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module BasicOperationTests where

import           Control.Monad.State
import           Data.Map                   (Map)
import qualified Data.Map                as M
import qualified Data.ByteString         as BS
import qualified Data.Text               as T
import           DynamoDbEventStore.Testing
import           EventStoreCommands
import           Test.Tasty.HUnit
import           Test.Tasty

testStreamId :: StreamId
testStreamId  = StreamId "Brownie"

testKey :: EventKey
testKey = EventKey (testStreamId, 0)

sampleBody = BS.singleton 1
sampleWrite :: EventStoreCmdM EventWriteResult
sampleWrite = writeEvent' testKey "FooCreatedEvent" sampleBody

sampleRead :: EventStoreCmdM EventReadResult
sampleRead = getEvent' testKey

tests :: (forall a. EventStoreCmdM a -> IO a) -> [TestTree]
tests evalProgram =
  [
    testCase "Write event returns WriteExists when event already exists" $
        let
          actions = do
            sampleWrite
            sampleWrite -- duplicate
          writeResult = evalProgram actions
        in do
          r <- writeResult
          assertEqual "Second write has error" EventExists r
    , testCase "Can read event" $
        let
          actions = do
            sampleWrite
            sampleRead
          evt = evalProgram actions
          expected :: Maybe (EventType, BS.ByteString, Maybe a)
          expected = Just ("FooCreatedEvent", sampleBody, Nothing)
        in do
          r <- evt
          assertEqual "Event is read" expected r
    , testCase "Can read events backward" $
        let
          actions = do
            writeEvent' (EventKey (testStreamId, 0)) "EventType1" sampleBody
            writeEvent' (EventKey (testStreamId, 1)) "EventType2" sampleBody
            getEventsBackward' testStreamId 10 Nothing
          evt = evalProgram actions
          expected :: [RecordedEvent]
          expected = [
            RecordedEvent "Brownie" 1 sampleBody "EventType2",
            RecordedEvent "Brownie" 0 sampleBody "EventType1" ]
        in do
          r <- evt
          assertEqual "Events are returned in reverse order" expected r
    , testCase "Set event page" $
      let
        actions = do
          sampleWrite
          r <- setEventPage' testKey (0,0)
          sampleRead
        evt = evalProgram actions
        expected = Just ("FooCreatedEvent", sampleBody, Just (0,0))
      in do
        r <- evt
        assertEqual "Page is set" expected r
    , testCase "Scan unpaged events returns nothing for empty event store" $
      let
        evtList = evalProgram scanUnpagedEvents'
      in do
        r <- evtList
        assertEqual "No items" [] r
    , testCase "Scan unpaged events returns written event" $
      let
        actions = do
          sampleWrite
          scanUnpagedEvents'
        evtList = evalProgram actions
      in do
        r <- evtList
        assertEqual "Should should have single item" [testKey] r
    , testCase "Scan unpaged events does not returned paged event" $
      let
        actions = do
          sampleWrite
          setEventPage' testKey (0,0)
          scanUnpagedEvents'
        evtList = evalProgram actions
      in do
        r <- evtList
        assertEqual "Should have no items" [] r
    , testCase "Writing page entry with wrong version should return error" $
      let
        pageKey = (0,0)
        actions = do
          writePageEntry' pageKey
                          PageWriteRequest {
                               expectedStatus = Nothing,
                               newStatus = Version 0,
                               entries = []}
          writePageEntry' pageKey
                          PageWriteRequest {
                               expectedStatus = Nothing,
                               newStatus = Version 0,
                               entries = []}
        r = evalProgram actions
      in do
        r' <- r
        assertEqual "Result should be nothing" Nothing r'
    , testCase "Page entry is updated when written with subsequent version" $
      let
        pageKey = (0,0)
        actions = do
          writePageEntry' pageKey
                          PageWriteRequest {
                               expectedStatus = Nothing,
                               newStatus = Version 0,
                               entries = []}
          writePageEntry' pageKey
                          PageWriteRequest {
                               expectedStatus = Just $ Version 0,
                               newStatus = Version 1,
                               entries = [testKey]}
          getPageEntry' pageKey
        r = evalProgram actions
      in do
        r' <- r
        assertEqual "Result should be Version 1" (Just (Version 1, [testKey])) r'
    , testCase "Written page entry should be returned by get" $
      let
        pageKey = (0,0)
        actions = do
          writeResult <- writePageEntry' pageKey
                          PageWriteRequest {
                               expectedStatus = Nothing,
                               newStatus = Version 0,
                               entries = [testKey]}
          readResult <- getPageEntry' pageKey
          return (writeResult, readResult)
        r = evalProgram actions
      in do
        r' <- r
        assertEqual "Read and write should return Version 0" (Just $ Version 0, Just (Version 0, [testKey])) r'
  ]
