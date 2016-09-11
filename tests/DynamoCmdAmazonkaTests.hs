{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module DynamoCmdAmazonkaTests where

import BasicPrelude
import Control.Lens
import qualified Data.HashMap.Lazy as HM
import qualified Data.Sequence as Seq
import DynamoDbEventStore.AmazonkaImplementation (InterpreterError)
import qualified DynamoDbEventStore.Constants as Constants
import DynamoDbEventStore.EventStoreCommands
import Network.AWS.DynamoDB hiding (updateItem)
import Test.Tasty
import Test.Tasty.HUnit

testStreamId :: Text
testStreamId = "Brownie"

testKey :: DynamoKey
testKey = DynamoKey testStreamId 0

sampleValuesNeedsPaging :: DynamoValues
sampleValuesNeedsPaging = 
    HM.singleton "Body" (set avS (Just "Andrew") attributeValue) &
    HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue)

sampleValuesNoPaging :: DynamoValues
sampleValuesNoPaging = 
    HM.singleton "Body" (set avS (Just "Andrew") attributeValue)

testWrite
    :: MonadEsDsl m
    => DynamoValues -> DynamoVersion -> m DynamoWriteResult
testWrite = writeToDynamo testKey

sampleRead
    :: MonadEsDsl m
    => m (Maybe DynamoReadResult)
sampleRead = readFromDynamo testKey

waitForItem
    :: (MonadEsDslWithFork m, Typeable a)
    => QueueType m a -> m a
waitForItem q = go Nothing
  where
    go Nothing = tryReadQueue q >>= go
    go (Just x) = return x

tests
    :: MonadEsDslWithFork m
    => (forall a. m a -> IO (Either InterpreterError a)) -> [TestTree]
tests evalProgram = 
    [ testCase "Can spawn a new thread" $
      let queueItem = (PageKey 1, Seq.fromList ['a'])
          childThread
              :: MonadEsDsl m
              => QueueType m (PageKey, Seq Char) -> m ()
          childThread q = do
              writeQueue q queueItem
          actions = do
              q <- newQueue
              forkChild "childThread" (childThread q)
              waitForItem q
          evt = evalProgram actions
          expected = Right queueItem
      in do r <- evt
            assertEqual "Queue item is read back" expected r
    , testCase "Can write and read to complete page queue" $
      let queueItem = (PageKey 1, Seq.fromList ['a'])
          actions = do
              q <- newQueue
              writeQueue q queueItem >> tryReadQueue q
          evt = evalProgram actions
          expected = Right . Just $ queueItem
      in do r <- evt
            assertEqual "Queue item is read back" expected r
    , testCase "Can read from cache" $
      let actions = do
              c <- newCache 10
              cacheInsert c 'a' 'b'
              cacheLookup c 'a'
          result = evalProgram actions
          expected = Right . Just $ 'b'
      in do r <- result
            assertEqual "Cache item is read back" expected r
    , testCase "Can read event" $
      let actions = do
              _ <- testWrite sampleValuesNeedsPaging 0
              sampleRead
          evt = evalProgram actions
          expected = 
              Right $ Just $ DynamoReadResult testKey 0 sampleValuesNeedsPaging
      in do r <- evt
            assertEqual "Event is read" expected r
    , testCase "Write event returns WriteExists when event already exists" $
      let actions = do
              _ <- testWrite sampleValuesNeedsPaging 0
              testWrite sampleValuesNeedsPaging 0 -- duplicate
          writeResult = evalProgram actions
      in do r <- writeResult
            assertEqual
                "Second write has error"
                (Right DynamoWriteWrongVersion)
                r
    , testCase "Update set field adds the field" $
      let myKeyValue = set avS (Just "testValue") attributeValue
          actions = do
              _ <- testWrite sampleValuesNoPaging 0
              _ <- 
                  updateItem
                      testKey
                      (HM.singleton "MyKey" (ValueUpdateSet myKeyValue))
              sampleRead
          readResult = evalProgram actions
      in do r <- readResult
            let myKey = (HM.lookup "MyKey" . dynamoReadResultValue <$>) <$> r
            assertEqual
                "MyKey as value: testValue"
                (Right . Just . Just $ myKeyValue)
                myKey
    , testCase "Delete field removes the field" $
      let actions = do
              _ <- testWrite sampleValuesNeedsPaging 0
              _ <- 
                  updateItem
                      testKey
                      (HM.singleton Constants.needsPagingKey ValueUpdateDelete)
              sampleRead
          readResult = evalProgram actions
      in do r <- readResult
            let needsPagingKey = 
                    (HM.lookup Constants.needsPagingKey . dynamoReadResultValue <$>) <$>
                    r
            assertEqual
                "NeedsPaging has been deleted"
                (Right . Just $ Nothing)
                needsPagingKey
    , testCase "With correct version you can write a subsequent event" $
      let actions = do
              _ <- testWrite sampleValuesNeedsPaging 0
              testWrite sampleValuesNeedsPaging 1
          writeResult = evalProgram actions
      in do r <- writeResult
            assertEqual
                "Second write should succeed"
                (Right DynamoWriteSuccess)
                r
    , testCase "Scan unpaged events returns written event" $
      let actions = do
              _ <- testWrite sampleValuesNeedsPaging 0
              scanNeedsPaging
          evtList = evalProgram actions
      in do r <- evtList
            assertEqual "Should should have single item" (Right [testKey]) r
    , testCase "Scan unpaged events does not returned paged event" $
      let actions = do
              _ <- testWrite sampleValuesNeedsPaging 0
              _ <- testWrite sampleValuesNoPaging 1
              scanNeedsPaging
          evtList = evalProgram actions
      in do r <- evtList
            assertEqual "Should have no items" (Right []) r
    , testCase "Can read events backward" $
      let actions = do
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 0)
                      sampleValuesNeedsPaging
                      0
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 1)
                      sampleValuesNeedsPaging
                      0
              queryTable QueryDirectionBackward testStreamId 10 Nothing
          evt = evalProgram actions
          expected = 
              Right
                  [ DynamoReadResult
                        (DynamoKey testStreamId 1)
                        0
                        sampleValuesNeedsPaging
                  , DynamoReadResult
                        (DynamoKey testStreamId 0)
                        0
                        sampleValuesNeedsPaging]
      in do r <- evt
            assertEqual "Events are returned in reverse order" expected r
    , testCase "Read events respects max items " $
      let actions = do
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 0)
                      sampleValuesNeedsPaging
                      0
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 1)
                      sampleValuesNeedsPaging
                      0
              queryTable QueryDirectionBackward testStreamId 1 Nothing
          evt = evalProgram actions
          expected = 
              Right
                  [ DynamoReadResult
                        (DynamoKey testStreamId 1)
                        0
                        sampleValuesNeedsPaging]
      in do r <- evt
            assertEqual "Only event 1 should be returned" expected r
    , testCase "Can read events backward starting at offset" $
      let actions = do
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 0)
                      sampleValuesNeedsPaging
                      0
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 1)
                      sampleValuesNeedsPaging
                      0
              queryTable QueryDirectionBackward testStreamId 10 (Just 1)
          evt = evalProgram actions
          expected = 
              Right
                  [ DynamoReadResult
                        (DynamoKey testStreamId 0)
                        0
                        sampleValuesNeedsPaging]
      in do r <- evt
            assertEqual "Only the 0th event is returned" expected r
    , testCase "Can read events forward starting at offset" $
      let actions = do
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 0)
                      sampleValuesNeedsPaging
                      0
              _ <- 
                  writeToDynamo
                      (DynamoKey testStreamId 1)
                      sampleValuesNeedsPaging
                      0
              queryTable QueryDirectionForward testStreamId 10 (Just 0)
          evt = evalProgram actions
          expected = 
              Right
                  [ DynamoReadResult
                        (DynamoKey testStreamId 1)
                        0
                        sampleValuesNeedsPaging]
      in do r <- evt
            assertEqual "Only the 1st event is returned" expected r]
