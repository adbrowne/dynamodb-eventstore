{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
module WebserverSpec (postEventSpec) where

import           Test.Tasty.Hspec
import           Network.Wai.Test
import           Network.HTTP.Types (methodPost)
import           Network.Wai
import qualified Webserver as W
import qualified Web.Scotty as S
import qualified Network.HTTP.Types as H
import           Control.Applicative (pure)

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
      waiCase requestWithExpectedVersion $ assertBody "PostEvent (PostEventRequest {streamId = \"streamId\", expectedVersion = 1, eventData = \"\"})"

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
    app = S.scottyApp (W.app W.showEventResponse)
    waiCase r assertion = do
      app' <- app
      flip runSession app' $ assertion =<< r

