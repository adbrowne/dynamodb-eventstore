{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           Control.Monad.Free
import           Control.Monad.State
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.List               as L
import           Data.Map                (Map)
import qualified Data.Map                as M
import qualified Data.Set                as S
import           Data.Int
import qualified Data.Text               as T
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import           EventStoreActions
import           EventStoreCommands
import qualified BasicOperationTests     as BasicOps
import           DynamoDbEventStore.Testing

import           Test.Tasty
import           Test.Tasty.Hspec
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import qualified WebserverInternalSpec
import           WebserverSpec

newtype SingleStreamValidActions = SingleStreamValidActions [PostEventRequest] deriving (Show)

instance Arbitrary PostEventRequest where
  arbitrary = liftM3 PostEventRequest (fmap TL.pack arbitrary) arbitrary (fmap BL.pack arbitrary)

instance Arbitrary SingleStreamValidActions where
  arbitrary = do
    eventList <- listOf arbitrary
    let (_, numberedEventList) = L.mapAccumL numberEvent 0 eventList
    return $ SingleStreamValidActions numberedEventList
    where
      numberEvent i e = (i+1,e { expectedVersion = i })

toRecordedEvent (PostEventRequest sId v d) = RecordedEvent {
  recordedEventStreamId = sId,
  recordedEventNumber = v,
  recordedEventData = d }

runActions :: [PostEventRequest] -> Gen [RecordedEvent]
runActions a =
  let
    s = L.foldl' runItem M.empty a
    events = M.assocs s
  in
    elements $ [fmap toRecEvent events]
  where
    toRecEvent :: (EventKey, (EventType, BS.ByteString, Maybe PageKey)) -> RecordedEvent
    toRecEvent (EventKey (StreamId sId, version),(eventType, body, _)) = RecordedEvent {
          recordedEventStreamId = TL.fromStrict sId,
          recordedEventNumber = version,
          recordedEventData = BL.fromStrict body }

prop_AllEventsAppearInSubscription (SingleStreamValidActions actions) =
  forAll (runActions actions) $ \r ->
    (S.fromList r) === S.fromList (map toRecordedEvent actions)

main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "Unit Tests"
        BasicOps.tests,
        postEventSpec',
        webserverInternalTests',
        testProperty "All Events Appear in Subscription" prop_AllEventsAppearInSubscription
      ]
