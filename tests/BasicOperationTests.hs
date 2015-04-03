{-# LANGUAGE OverloadedStrings #-}

module BasicOperationTests where

import           Control.Monad.State
import           Data.Map                   (Map)
import qualified Data.Map                as M
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text.Lazy          as TL
import           DynamoDbEventStore.Testing
import           EventStoreCommands
import           Test.Tasty.HUnit

testKey :: EventKey
testKey = EventKey ((StreamId "Browne"), 0)

sampleWrite :: EventStoreCmdM EventWriteResult
sampleWrite = do
  writeEvent' testKey "FooCreatedEvent" BS.empty

tests =
    [ testCase "Can write event" $
      let
        (_,s) = runState (runTest sampleWrite) M.empty
        expected = M.singleton testKey ("FooCreatedEvent", BS.empty, Nothing)
      in
        assertEqual "Event is in the map" expected s
    ]
