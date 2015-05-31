{-# LANGUAGE OverloadedStrings #-}

module DynamoDbEventStore.EventStoreActionTests where

import           Control.Monad.State
import           Data.Map                (Map)
import qualified Data.Map                as M
import qualified Data.List               as L
import qualified Data.Set                as S
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text.Lazy          as TL
import qualified Data.Text               as T
import           DynamoDbEventStore.Testing
import           EventStoreActions
import           EventStoreCommands
import           Test.Tasty
import           Test.Tasty.QuickCheck

runItem :: FakeState -> PostEventRequest -> FakeState
runItem state (PostEventRequest sId v d et) =
  let
   (_, s) = runState (runTest writeItem) state
  in s
  where
      writeItem =
        writeEvent' (EventKey (StreamId (TL.toStrict sId),v))
          et
          (BL.toStrict d)

newtype SingleStreamValidActions = SingleStreamValidActions [PostEventRequest] deriving (Show)

instance Arbitrary PostEventRequest where
  arbitrary = liftM4 PostEventRequest (fmap TL.pack arbitrary) arbitrary (fmap BL.pack arbitrary) (fmap T.pack arbitrary)

instance Arbitrary SingleStreamValidActions where
  arbitrary = do
    eventList <- listOf arbitrary
    let (_, numberedEventList) = L.mapAccumL numberEvent 0 eventList
    return $ SingleStreamValidActions numberedEventList
    where
      numberEvent i e = (i+1,e { expectedVersion = i })

toRecordedEvent (PostEventRequest sId v d et) = RecordedEvent {
  recordedEventStreamId = sId,
  recordedEventNumber = v,
  recordedEventData = d,
  recordedEventType = et }

runActions :: [PostEventRequest] -> Gen [RecordedEvent]
runActions a =
  let
    s = L.foldl' runItem emptyTestState a
    events = (M.assocs . fst) s
  in
    elements [fmap toRecEvent events]
  where
    toRecEvent :: (EventKey, (EventType, BS.ByteString, Maybe PageKey)) -> RecordedEvent
    toRecEvent (EventKey (StreamId sId, version),(eventType, body, _)) = RecordedEvent {
          recordedEventStreamId = TL.fromStrict sId,
          recordedEventNumber = version,
          recordedEventData = BL.fromStrict body,
          recordedEventType = eventType }
prop_AllEventsAppearInSubscription (SingleStreamValidActions actions) =
  forAll (runActions actions) $ \r ->
    S.fromList r === S.fromList (map toRecordedEvent actions)

tests :: [TestTree]
tests = [
      testProperty "All Events Appear in Subscription" prop_AllEventsAppearInSubscription
  ]
