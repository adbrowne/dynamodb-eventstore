{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module WebserverSpec (postEventSpec, getStreamSpec, getEventSpec) where

import           BasicPrelude
import           Control.Monad.Reader
import qualified Data.Text.Lazy                       as TL
import           Data.Time.Clock
import           Data.Time.Format
import           DynamoDbEventStore.EventStoreActions (EventStoreAction)
import qualified DynamoDbEventStore.Webserver         as W
import qualified Network.HTTP.Types                   as H
import           Network.Wai
import           Network.Wai.Test
import           Test.Tasty.Hspec
import qualified Web.Scotty.Trans                     as S

addEventPost :: [H.Header] -> Session SResponse
addEventPost headers =
  request $ defaultRequest {
               pathInfo = ["streams","streamId"],
               requestMethod = H.methodPost,
               requestHeaders = headers,
               requestBody = pure "" }

evHeader :: H.HeaderName
evHeader = "ES-ExpectedVersion"
etHeader :: H.HeaderName
etHeader = "ES-EventType"
eventIdHeader :: H.HeaderName
eventIdHeader = "ES-EventId"

showEventResponse :: (Monad m, S.ScottyError e) => EventStoreAction -> S.ActionT e m ()
showEventResponse eventStoreAction = S.text $ TL.fromStrict $ show eventStoreAction

app :: IO Application
app = do
  sampleTime <- parseTimeM True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000"
  S.scottyAppT (`runReaderT` sampleTime) (W.app showEventResponse :: S.ScottyT LText (ReaderT UTCTime IO) ())

postEventSpec :: Spec
postEventSpec = do
  let baseHeaders = [
        (etHeader, "MyEventType"),
        (eventIdHeader, "12f44004-f5dd-41f1-8225-72dd65a0332e"),
        ("Content-Type", "application/json")]
  let requestWithExpectedVersion = addEventPost $ (evHeader, "1"):baseHeaders
  let requestWithoutExpectedVersion = addEventPost baseHeaders
  let requestWithoutBadExpectedVersion = addEventPost $ (evHeader, "NotAnInt"):baseHeaders
  let requestWithoutEventType = addEventPost [(evHeader, "1")]

  describe "Parse Int64 header" $ do
    it "responds with 200" $
      waiCase requestWithExpectedVersion $ assertStatus 200

    it "responds with body" $
      waiCase requestWithExpectedVersion $ assertBody "PostEvent (PostEventRequest {perStreamId = \"streamId\", perExpectedVersion = Just 1, perEvents = EventEntry {eventEntryData = \"\", eventEntryType = EventType \"MyEventType\", eventEntryEventId = EventId {unEventId = 12f44004-f5dd-41f1-8225-72dd65a0332e}, eventEntryCreated = EventTime 2016-05-08 12:49:41 UTC, eventEntryIsJson = True} :| []})"

  describe "POST /streams/streamId without ExepectedVersion" $ do
    it "responds with 200" $
      waiCase requestWithoutExpectedVersion $ assertStatus 200

    it "responds with body" $
      waiCase requestWithoutExpectedVersion $ assertBody "PostEvent (PostEventRequest {perStreamId = \"streamId\", perExpectedVersion = Nothing, perEvents = EventEntry {eventEntryData = \"\", eventEntryType = EventType \"MyEventType\", eventEntryEventId = EventId {unEventId = 12f44004-f5dd-41f1-8225-72dd65a0332e}, eventEntryCreated = EventTime 2016-05-08 12:49:41 UTC, eventEntryIsJson = True} :| []})"

  describe "POST /streams/streamId without EventType" $
    it "responds with 400" $
      waiCase requestWithoutEventType $ assertStatus 400

  describe "POST /streams/streamId without ExepectedVersion greater than Int64.max" $
    it "responds with 400" $
       addEventPost [("ES-ExpectedVersion", "9223372036854775808")] `waiCase` assertStatus 400

  describe "POST /streams/streamId with non-integer ExpectedVersion" $
    it "responds with 400" $
      requestWithoutBadExpectedVersion `waiCase` assertStatus 400

getStream :: Text -> Session SResponse
getStream streamId =
  request $ defaultRequest {
               pathInfo = ["streams",streamId],
               requestMethod = H.methodGet
            }

assertSuccess :: String -> [Text] -> LByteString -> Spec
assertSuccess desc path expectedType =
  describe ("Get " <> desc) $ do
    let getExample = request $ defaultRequest {
               pathInfo = path,
               requestMethod = H.methodGet
            }

    it "responds with 200" $
      waiCase getExample $ assertStatus 200

    it "responds with body" $
      waiCase getExample $ assertBody expectedType

getStreamSpec :: Spec
getStreamSpec = do
  assertSuccess
    "stream simple"
    ["streams","myStreamId"]
    "ReadStream (ReadStreamRequest {rsrStreamId = StreamId \"myStreamId\", rsrStartEventNumber = Nothing, rsrMaxItems = 20, rsrDirection = FeedDirectionBackward})"

  assertSuccess
    "stream $all simple"
    ["streams","%24all"]
    "ReadAll (ReadAllRequest {readAllRequestStartPosition = Nothing, readAllRequestMaxItems = 20, readAllRequestDirection = FeedDirectionBackward})"

  assertSuccess
    "stream with start event and limit"
    ["streams","myStreamId","3","5"]
    "ReadStream (ReadStreamRequest {rsrStreamId = StreamId \"myStreamId\", rsrStartEventNumber = Just 3, rsrMaxItems = 5, rsrDirection = FeedDirectionBackward})"

  assertSuccess
    "stream $all with start event and limit"
    ["streams","%24all","3-2","5"]
    "ReadAll (ReadAllRequest {readAllRequestStartPosition = Just (GlobalFeedPosition {globalFeedPositionPage = 3, globalFeedPositionOffset = 2}), readAllRequestMaxItems = 5, readAllRequestDirection = FeedDirectionBackward})"

  assertSuccess
    "stream with start and limit, backward"
    ["streams","myStreamId","3","backward","5"]
    "ReadStream (ReadStreamRequest {rsrStreamId = StreamId \"myStreamId\", rsrStartEventNumber = Just 3, rsrMaxItems = 5, rsrDirection = FeedDirectionBackward})"

  assertSuccess
    "stream $all with start and limit, backward"
    ["streams","%24all","3-2","backward","5"]
    "ReadAll (ReadAllRequest {readAllRequestStartPosition = Just (GlobalFeedPosition {globalFeedPositionPage = 3, globalFeedPositionOffset = 2}), readAllRequestMaxItems = 5, readAllRequestDirection = FeedDirectionBackward})"

  assertSuccess
    "stream backward from head"
    ["streams","myStreamId","head","backward","5"]
    "ReadStream (ReadStreamRequest {rsrStreamId = StreamId \"myStreamId\", rsrStartEventNumber = Nothing, rsrMaxItems = 5, rsrDirection = FeedDirectionBackward})"

  assertSuccess
    "stream $all backward from head"
    ["streams","%24all","head","backward","5"]
    "ReadAll (ReadAllRequest {readAllRequestStartPosition = Nothing, readAllRequestMaxItems = 5, readAllRequestDirection = FeedDirectionBackward})"

  assertSuccess
    "stream with start and limit, forward"
    ["streams","myStreamId","3","forward","5"]
    "ReadStream (ReadStreamRequest {rsrStreamId = StreamId \"myStreamId\", rsrStartEventNumber = Just 3, rsrMaxItems = 5, rsrDirection = FeedDirectionForward})"

  assertSuccess
    "stream $all with start and limit, forward"
    ["streams","%24all","3-2","forward","5"]
    "ReadAll (ReadAllRequest {readAllRequestStartPosition = Just (GlobalFeedPosition {globalFeedPositionPage = 3, globalFeedPositionOffset = 2}), readAllRequestMaxItems = 5, readAllRequestDirection = FeedDirectionForward})"

  describe "Get stream with missing stream name" $ do
    let getExample = getStream ""
    it "responds with 400" $
      waiCase getExample $ assertStatus 400

getEvent :: Text -> Int64 -> Session SResponse
getEvent streamId eventNumber =
  request $ defaultRequest {
               pathInfo = ["streams",streamId,show eventNumber],
               requestMethod = H.methodGet
            }

getEventSpec :: Spec
getEventSpec =
  describe "Get stream" $ do
    let getExample = getEvent "myStreamId" 0
    it "responds with 200" $
      waiCase getExample $ assertStatus 200

    it "responds with body" $
      waiCase getExample $ assertBody "ReadEvent (ReadEventRequest {rerStreamId = \"myStreamId\", rerEventNumber = 0})"

waiCase :: Session SResponse -> (SResponse -> Session ()) -> IO ()
waiCase r assertion = do
  app' <- app
  flip runSession app' $ assertion =<< r
