{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module DynamoCmdAmazonkaTests where

import qualified Data.ByteString         as BS
import qualified Data.HashMap.Lazy       as HM
import qualified Data.Text               as T
import           Network.AWS.DynamoDB
import           EventStoreCommands
import           Control.Lens
import           Test.Tasty.HUnit
import           Test.Tasty
import qualified DynamoDbEventStore.Constants as Constants

testStreamId :: T.Text
testStreamId  = "Brownie"

testKey :: DynamoKey
testKey = DynamoKey testStreamId 0

sampleValuesNeedsPaging :: DynamoValues
sampleValuesNeedsPaging = HM.singleton "Body" (set avS (Just "Andrew") attributeValue) & HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue)

sampleValuesNoPaging :: DynamoValues
sampleValuesNoPaging = HM.singleton "Body" (set avS (Just "Andrew") attributeValue)

testWrite :: DynamoValues -> DynamoVersion -> DynamoCmdM DynamoWriteResult
testWrite = writeToDynamo' testKey

sampleRead :: DynamoCmdM (Maybe DynamoReadResult)
sampleRead = readFromDynamo' testKey

tests :: (forall a. DynamoCmdM a -> IO a) -> [TestTree]
tests evalProgram =
  [
    testCase "Can read event" $
        let
          actions = do
            _ <- testWrite sampleValuesNeedsPaging 0
            sampleRead
          evt = evalProgram actions
          expected :: Maybe DynamoReadResult
          expected = Just $ DynamoReadResult testKey 0 sampleValuesNeedsPaging
        in do
          r <- evt
          assertEqual "Event is read" expected r
    , testCase "Write event returns WriteExists when event already exists" $
        let
          actions = do
            _ <- testWrite sampleValuesNeedsPaging 0
            testWrite sampleValuesNeedsPaging 0 -- duplicate
          writeResult = evalProgram actions
        in do
          r <- writeResult
          assertEqual "Second write has error" DynamoWriteWrongVersion r
    , testCase "With correct version you can write a subsequent event" $
        let
          actions = do
            _ <- testWrite sampleValuesNeedsPaging 0
            testWrite sampleValuesNeedsPaging 1
          writeResult = evalProgram actions
        in do
          r <- writeResult
          assertEqual "Second write should succeed" DynamoWriteSuccess r
    , testCase "Scan unpaged events returns written event" $
      let
        actions = do
          _ <- testWrite sampleValuesNeedsPaging 0
          scanNeedsPaging'
        evtList = evalProgram actions
      in do
        r <- evtList
        assertEqual "Should should have single item" [testKey] r
    , testCase "Scan unpaged events does not returned paged event" $
      let
        actions = do
          _ <- testWrite sampleValuesNeedsPaging 0
          _ <- testWrite sampleValuesNoPaging 1
          scanNeedsPaging'
        evtList = evalProgram actions
      in do
        r <- evtList
        assertEqual "Should have no items" [] r
  {-
    , testCase "Can read events backward" $
        let
          actions = do
            _ <- writeEvent' (EventKey (testStreamId, 0)) "EventType1" sampleBody
            _ <- writeEvent' (EventKey (testStreamId, 1)) "EventType2" sampleBody
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
          _ <- sampleWrite
          _ <- setEventPage' testKey (0,0)
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
    , testCase "Writing page entry with wrong version should return error" $
      let
        pageKey = (0,0)
        actions = do
          _ <- writePageEntry' pageKey
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
          _ <- writePageEntry' pageKey
                          PageWriteRequest {
                               expectedStatus = Nothing,
                               newStatus = Version 0,
                               entries = []}
          _ <- writePageEntry' pageKey
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
        -}
  ]
