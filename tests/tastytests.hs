{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           Control.Monad.Free
import           Control.Monad.State
import qualified Data.ByteString     as BS
import           Data.Map            (Map)
import qualified Data.Map            as M
import           EventStoreCommands
import           Network.HTTP.Types  (methodPost)
import qualified Network.HTTP.Types as H
import           Network.Wai
import           Network.Wai.Test
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.Hspec
import qualified Data.ByteString.Lazy as BL
import           Control.Applicative (pure)
import qualified Webserver as W
import qualified Web.Scotty as S

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

addEventPost :: [H.Header] -> Session SResponse
addEventPost headers =
  request $ defaultRequest {
               pathInfo = ["streams","streamId"],
               requestMethod = methodPost,
               requestHeaders = headers,
               requestBody = pure "" }

postEventSpec :: Spec
postEventSpec = do
  let requestWithExpectedVersion = addEventPost [("ES-ExpectedVersion", "1")]
  let requestWithoutExpectedVersion = addEventPost []
  let requestWithoutBadExpectedVersion = addEventPost [("ES-ExepectedVersion", "NotAnInt")]

  describe "Parse Int64 header" $ do
    it "responds with 200" $
      waiCase requestWithExpectedVersion $ assertStatus 200

    it "responds with body" $
      waiCase requestWithExpectedVersion $ assertBody "StreamId:streamId expectedVersion:1"

  describe "POST /streams/streamId without ExepectedVersion" $ do
    it "responds with 400" $
      waiCase requestWithoutExpectedVersion $ assertStatus 400

  describe "POST /streams/streamId without ExepectedVersion greater than Int64.max" $ do
    it "responds with 400" $
       (addEventPost [("ES-ExpectedVersion", "9223372036854775808")]) `waiCase` (assertStatus 400)

  describe "POST /streams/streamId with non-integer ExpectedVersion" $ do
    it "responds with 400" $
      requestWithoutBadExpectedVersion `waiCase` (assertStatus 400)
  where
    app = S.scottyApp W.app
    waiCase request assertion = do
      app' <- app
      flip runSession app' $ assertion =<< request

main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
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
        postEventSpec'
      ]
