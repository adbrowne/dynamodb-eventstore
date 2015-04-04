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

tests =
  [
    testCase "Can write event" $
        let
          state = execProgram sampleWrite
          expected = M.singleton testKey ("FooCreatedEvent", BS.empty, Nothing)
        in
          assertEqual "Event is in the map" expected state
    , testCase "Write event returns WriteExists when event already exists" $
        let
          actions = do
            sampleWrite
            sampleWrite -- duplicate
          writeResult = evalProgram actions
        in
          assertEqual "Second write has error" EventExists writeResult
    , testCase "Can read event" $
        let
          actions = do
            sampleWrite
            sampleRead
          evt = evalProgram actions
          expected = Just ("FooCreatedEvent", BS.empty, Nothing)
        in
          assertEqual "Event is read" expected evt
    , testCase "Set event page" $
      let
        actions = do
          sampleWrite
          r <- setEventPage' testKey (0,0)
          sampleRead
        evt = evalProgram actions
        expected = Just ("FooCreatedEvent", BS.empty, Just (0,0))
      in
        assertEqual "Page is set" expected evt
  ]
