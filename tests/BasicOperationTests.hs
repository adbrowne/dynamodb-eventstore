{-# LANGUAGE OverloadedStrings #-}

module BasicOperationTests where

import           Control.Monad.State
import           Data.Map                   (Map)
import qualified Data.Map                as M
import qualified Data.ByteString         as BS
import           DynamoDbEventStore.Testing
import           EventStoreCommands
import           Test.Tasty.HUnit

testKey :: EventKey
testKey = EventKey ((StreamId "Browne"), 0)

sampleWrite :: EventStoreCmdM EventWriteResult
sampleWrite = writeEvent' testKey "FooCreatedEvent" BS.empty

sampleRead :: EventStoreCmdM EventReadResult
sampleRead = getEvent' testKey

test_writeEvent =
  testCase "Can write event" $
      let
        (_,s) = runState (runTest sampleWrite) M.empty
        expected = M.singleton testKey ("FooCreatedEvent", BS.empty, Nothing)
      in
        assertEqual "Event is in the map" expected s

test_readEvent =
  testCase "Can read event" $
    let
      (_,s) = runState (runTest sampleWrite) M.empty
      (evt,_) = runState (runTest sampleRead) s
      expected = Just ("FooCreatedEvent", BS.empty, Nothing)
    in
      assertEqual "Event is read" expected evt

tests =
    [
      test_writeEvent,
      test_readEvent
    ]
