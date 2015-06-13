{-# LANGUAGE OverloadedStrings #-}

module DynamoDbEventStore.EventStoreActionTests where

import           Control.Monad.State
import qualified Data.Map                as M
import qualified Data.List               as L
import qualified Data.Set                as S
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text               as T
import           DynamoDbEventStore.Testing
import           EventStoreActions
import           EventStoreCommands
import           Test.Tasty
import           Test.Tasty.QuickCheck

runItem :: FakeState -> PostEventRequest -> FakeState
runItem fakeState (PostEventRequest sId v d et) =
  let
   (_, s) = runState (runTest writeItem) fakeState
  in s
  where
      writeItem =
        writeEvent' (EventKey (StreamId sId,v))
          et
          (BL.toStrict d)

newtype SingleStreamValidActions = SingleStreamValidActions [PostEventRequest] deriving (Show)

instance Arbitrary PostEventRequest where
  arbitrary = liftM4 PostEventRequest (fmap T.pack arbitrary) arbitrary (fmap BL.pack arbitrary) (fmap T.pack arbitrary)

instance Arbitrary SingleStreamValidActions where
  arbitrary = do
    eventList <- listOf arbitrary
    let (_, numberedEventList) = L.mapAccumL numberEvent 0 eventList
    return $ SingleStreamValidActions numberedEventList
    where
      numberEvent i e = (i+1,e { perExpectedVersion = i, perStreamId = "s" })

toRecordedEvent :: PostEventRequest -> RecordedEvent
toRecordedEvent (PostEventRequest sId v d et) = RecordedEvent {
  recordedEventStreamId = sId,
  recordedEventNumber = v,
  recordedEventData = BL.toStrict d,
  recordedEventType = et }

toEventKey :: PostEventRequest -> EventKey
toEventKey (PostEventRequest sId v d et) =
  EventKey (StreamId sId, v)

runActions :: [PostEventRequest] -> Gen ([EventKey])
runActions a =
  let
    s = L.foldl' runItem emptyTestState a
    (_,s') = runState (runTest (writePagesProgram $ Just 100)) s
    events = do
      (_, entries) <- (M.elems . snd) s'
      entries
  in
    elements [events]
  where
    toRecEvent :: (EventKey, (EventType, BS.ByteString, Maybe PageKey)) -> RecordedEvent
    toRecEvent (EventKey (StreamId sId, version),(eventType, body, _)) = RecordedEvent {
          recordedEventStreamId = sId,
          recordedEventNumber = version,
          recordedEventData = body,
          recordedEventType = eventType }

prop_AllEventsAppearInSubscription :: SingleStreamValidActions -> Property
prop_AllEventsAppearInSubscription (SingleStreamValidActions actions) =
  forAll (runActions actions) $ \r ->
    S.fromList r === S.fromList (map toEventKey actions)

prop_GlobalFeedPreservesEventOrdering :: SingleStreamValidActions -> Property
prop_GlobalFeedPreservesEventOrdering (SingleStreamValidActions actions) =
  forAll (runActions actions) $ \r ->
    r === map toEventKey actions

tests :: [TestTree]
tests = [
      testProperty "All Events Appear in Subscription" prop_AllEventsAppearInSubscription
      , testProperty "Global Feed preserves stream oder" prop_GlobalFeedPreservesEventOrdering
  ]
