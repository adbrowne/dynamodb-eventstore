{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import Test.Tasty
import Test.Tasty.HUnit
import DynamoCommands
import Data.Map (Map)
import qualified Data.Map as M
import Control.Monad.State
import Control.Monad.Free
import qualified Data.ByteString as BS

type FakeDB = Map EventKey (EventType, BS.ByteString)

runTest :: MonadState FakeDB m => DynamoCmdM a -> m a
runTest = iterM run
  where
    run (GetEvent' k f) = f =<< gets (M.lookup k)
    run (WriteEvent' k t v n) = do
      modify $ M.insert k (t,v)
      n WriteSuccess

testKey :: EventKey
testKey = EventKey ((StreamId "Browne"), 0)

sampleWrite :: DynamoCmdM EventWriteResult
sampleWrite = do
  writeEvent' testKey "FooCreatedEvent" BS.empty

main :: IO ()
main = defaultMain $
  testGroup "Tests"
    [ testGroup "Unit Tests"
        [ testCase "Can write event" $
          let
            (_,s) = runState (runTest sampleWrite) M.empty
            expected = M.singleton testKey ("FooCreatedEvent", BS.empty)
          in
            assertEqual "Event is in the map" expected s
        ]
    ]
