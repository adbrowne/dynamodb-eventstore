{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           Control.Monad.Free
import           Control.Monad.State
import qualified Data.ByteString     as BS
import           Data.Map            (Map)
import qualified Data.Map            as M
import           EventStoreCommands

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.Hspec
import           WebserverSpec
import qualified WebserverInternalSpec
type FakeDB = Map EventKey (EventType, BS.ByteString)

runTest :: MonadState FakeDB m => EventStoreCmdM a -> m a
runTest = iterM run
  where
    run (GetEvent' k f) = f =<< gets (M.lookup k)
    run (WriteEvent' k t v n) = do
      modify $ M.insert k (t,v)
      n WriteSuccess

testKey :: EventKey
testKey = EventKey ((StreamId "Browne"), 0)

sampleWrite :: EventStoreCmdM EventWriteResult
sampleWrite = do
  writeEvent' testKey "FooCreatedEvent" BS.empty

main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "Unit Tests"
          [ testCase "Can write event" $
            let
              (_,s) = runState (runTest sampleWrite) M.empty
              expected = M.singleton testKey ("FooCreatedEvent", BS.empty)
            in
              assertEqual "Event is in the map" expected s
          ],
        postEventSpec',
        webserverInternalTests'
      ]
