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
    testCase "Wait has no effect" $
        let
          state = execProgram wait'
        in
          assertEqual "Empty map" state emptyTestState
    , testCase "Can write event" $
        let
          state = execProgram sampleWrite
          expected = M.singleton testKey ("FooCreatedEvent", BS.empty, Nothing)
        in
          assertEqual "Event is in the map" (expected, M.empty) state
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
    , testCase "Scan unpaged events returns nothing for empty event store" $
      let
        actions = do
          scanUnpagedEvents'
        evtList = evalProgram actions
      in
        assertEqual "No items" [] evtList
    , testCase "Scan unpaged events returns written event" $
      let
        actions = do
          sampleWrite
          scanUnpagedEvents'
        evtList = evalProgram actions
      in
        assertEqual "Should should have single item" [testKey] evtList
    , testCase "Scan unpaged events does not returned paged event" $
      let
        actions = do
          sampleWrite
          setEventPage' testKey (0,0)
          scanUnpagedEvents'
        evtList = evalProgram actions
      in
        assertEqual "Should have no items" [] evtList
  ]
