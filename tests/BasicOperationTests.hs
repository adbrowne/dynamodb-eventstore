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
        s = execState (runTest sampleWrite) M.empty
        expected = M.singleton testKey ("FooCreatedEvent", BS.empty, Nothing)
      in
        assertEqual "Event is in the map" expected s

test_readEvent =
  testCase "Can read event" $
    let
      actions = do
        runTest sampleWrite
        runTest sampleRead
      evt = evalState actions M.empty
      expected = Just ("FooCreatedEvent", BS.empty, Nothing)
    in
      assertEqual "Event is read" expected evt

test_setEventPage =
  testCase "Set event page" $
    let
      actions = do
        runTest sampleWrite
        runTest $ setEventPage' testKey (0,0)
        runTest sampleRead
      evt = evalState actions M.empty
      expected = Just ("FooCreatedEvent", BS.empty, Just (0,0))
    in
      assertEqual "Page is set" expected evt

tests =
    [
      test_writeEvent,
      test_readEvent,
      test_setEventPage
    ]
