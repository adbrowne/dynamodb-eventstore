{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module DynamoCmdAmazonkaTests where

import           BasicPrelude
import qualified Data.HashMap.Lazy       as HM
import           Network.AWS.DynamoDB
import           DynamoDbEventStore.EventStoreCommands
import           Control.Lens
import           Test.Tasty.HUnit
import           Test.Tasty
import qualified DynamoDbEventStore.Constants as Constants

testStreamId :: Text
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

tests :: (forall a. DynamoCmdM a -> IO (Either Text a)) -> [TestTree]
tests evalProgram =
  [
    testCase "Can read event" $
        let
          actions = do
            _ <- testWrite sampleValuesNeedsPaging 0
            sampleRead
          evt = evalProgram actions
          expected = Right $ Just $ DynamoReadResult testKey 0 sampleValuesNeedsPaging
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
          assertEqual "Second write has error" (Right DynamoWriteWrongVersion) r
    , testCase "With correct version you can write a subsequent event" $
        let
          actions = do
            _ <- testWrite sampleValuesNeedsPaging 0
            testWrite sampleValuesNeedsPaging 1
          writeResult = evalProgram actions
        in do
          r <- writeResult
          assertEqual "Second write should succeed" (Right DynamoWriteSuccess) r
    , testCase "Scan unpaged events returns written event" $
      let
        actions = do
          _ <- testWrite sampleValuesNeedsPaging 0
          scanNeedsPaging'
        evtList = evalProgram actions
      in do
        r <- evtList
        assertEqual "Should should have single item" (Right [testKey]) r
    , testCase "Scan unpaged events does not returned paged event" $
      let
        actions = do
          _ <- testWrite sampleValuesNeedsPaging 0
          _ <- testWrite sampleValuesNoPaging 1
          scanNeedsPaging'
        evtList = evalProgram actions
      in do
        r <- evtList
        assertEqual "Should have no items" (Right []) r
    , testCase "Can read events backward" $
        let
          actions = do
            _ <- writeToDynamo' (DynamoKey testStreamId 0) sampleValuesNeedsPaging 0
            _ <- writeToDynamo' (DynamoKey testStreamId 1) sampleValuesNeedsPaging 0
            queryBackward' testStreamId 10 Nothing
          evt = evalProgram actions
          expected = Right [
            DynamoReadResult (DynamoKey testStreamId 1) 0 sampleValuesNeedsPaging,
            DynamoReadResult (DynamoKey testStreamId 0) 0 sampleValuesNeedsPaging ]
        in do
          r <- evt
          assertEqual "Events are returned in reverse order" expected r
    , testCase "Read events respects max items " $
        let
          actions = do
            _ <- writeToDynamo' (DynamoKey testStreamId 0) sampleValuesNeedsPaging 0
            _ <- writeToDynamo' (DynamoKey testStreamId 1) sampleValuesNeedsPaging 0
            queryBackward' testStreamId 1 Nothing
          evt = evalProgram actions
          expected = Right [
            DynamoReadResult (DynamoKey testStreamId 1) 0 sampleValuesNeedsPaging ]
        in do
          r <- evt
          assertEqual "Only event 1 should be returned" expected r
    , testCase "Can read events backward starting at offset" $
        let
          actions = do
            _ <- writeToDynamo' (DynamoKey testStreamId 0) sampleValuesNeedsPaging 0
            _ <- writeToDynamo' (DynamoKey testStreamId 1) sampleValuesNeedsPaging 0
            queryBackward' testStreamId 10 (Just 1)
          evt = evalProgram actions
          expected = Right [
            DynamoReadResult (DynamoKey testStreamId 0) 0 sampleValuesNeedsPaging ]
        in do
          r <- evt
          assertEqual "Only the 0th event is returned" expected r
  ]
