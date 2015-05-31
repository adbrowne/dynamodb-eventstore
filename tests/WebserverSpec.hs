{-# LANGUAGE OverloadedStrings #-}
module WebserverSpec (postEventSpec, getStreamSpec) where

import           Test.Tasty.Hspec
import           Network.Wai.Test
import           Network.HTTP.Types (methodPost)
import           Network.Wai
import qualified Webserver as W
import qualified Web.Scotty as S
import qualified Network.HTTP.Types as H
import           Control.Applicative (pure)
import qualified Data.Text            as T

addEventPost :: [H.Header] -> Session SResponse
addEventPost headers =
  request $ defaultRequest {
               pathInfo = ["streams","streamId"],
               requestMethod = H.methodPost,
               requestHeaders = headers,
               requestBody = pure "" }

getStream :: T.Text -> Session SResponse
getStream streamId =
  request $ defaultRequest {
               pathInfo = ["streams",streamId],
               requestMethod = H.methodGet
            }

evHeader = "ES-ExpectedVersion"
etHeader = "ES-EventType"

postEventSpec :: Spec
postEventSpec = do
  let baseHeaders = [(etHeader, "MyEventType")]
  let requestWithExpectedVersion = addEventPost $ (evHeader, "1"):baseHeaders
  let requestWithoutExpectedVersion = addEventPost baseHeaders
  let requestWithoutBadExpectedVersion = addEventPost $ (evHeader, "NotAnInt"):baseHeaders
  let requestWithoutEventType = addEventPost [(evHeader, "1")]

  describe "Parse Int64 header" $ do
    it "responds with 200" $
      waiCase requestWithExpectedVersion $ assertStatus 200

    it "responds with body" $
      waiCase requestWithExpectedVersion $ assertBody "PostEvent (PostEventRequest {streamId = \"streamId\", expectedVersion = 1, eventData = \"\", eventType = \"MyEventType\"})"

  describe "POST /streams/streamId without ExepectedVersion" $
    it "responds with 400" $
      waiCase requestWithoutExpectedVersion $ assertStatus 400

  describe "POST /streams/streamId without EventType" $
    it "responds with 400" $
      waiCase requestWithoutEventType $ assertStatus 400

  describe "POST /streams/streamId without ExepectedVersion greater than Int64.max" $
    it "responds with 400" $
       addEventPost [("ES-ExpectedVersion", "9223372036854775808")] `waiCase` assertStatus 400

  describe "POST /streams/streamId with non-integer ExpectedVersion" $
    it "responds with 400" $
      requestWithoutBadExpectedVersion `waiCase` assertStatus 400
  where
    app = S.scottyApp (W.app W.showEventResponse)
    waiCase r assertion = do
      app' <- app
      flip runSession app' $ assertion =<< r

getStreamSpec :: Spec
getStreamSpec = do
  describe "Get stream" $ do
    it "responds with 200" $
      waiCase (getStream "myStreamId") $ assertStatus 200

  where
    app = S.scottyApp (W.app W.showEventResponse)
    waiCase r assertion = do
      app' <- app
      flip runSession app' $ assertion =<< r
