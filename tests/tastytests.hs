{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           Control.Monad.Free
import           Control.Monad.State
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import           Data.Map                (Map)
import qualified Data.Map                as M
import qualified Data.Set                as S
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import           EventStoreActions
import           EventStoreCommands

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.Hspec
import           Test.Tasty.QuickCheck
import           WebserverSpec
import qualified WebserverInternalSpec

type FakeEventTable = Map EventKey (EventType, BS.ByteString, Maybe PageKey)
type FakePageTable = Map PageKey (PageStatus, [EventKey])

runTest :: MonadState FakeEventTable m => EventStoreCmdM a -> m a
runTest = iterM run
  where
    run (GetEvent' k f) = f =<< gets (M.lookup k)
    run (WriteEvent' k t v n) = do
      modify $ M.insert k (t,v, Nothing)
      n WriteSuccess

testKey :: EventKey
testKey = EventKey ((StreamId "Browne"), 0)

sampleWrite :: EventStoreCmdM EventWriteResult
sampleWrite = do
  writeEvent' testKey "FooCreatedEvent" BS.empty

prop_List :: [Int] -> Property
prop_List xs = (reverse . reverse) xs === xs

newtype SingleStreamValidActions = SingleStreamValidActions [PostEventRequest] deriving (Show)

instance Arbitrary PostEventRequest where
  arbitrary = liftM3 PostEventRequest (fmap TL.pack arbitrary) arbitrary (fmap BL.pack arbitrary)

instance Arbitrary SingleStreamValidActions where
  arbitrary = fmap SingleStreamValidActions (listOf $ arbitrary)
{--  arbitraray = rev $ sized actionList
    where
      actionList 0 = []
      actionList n =
        liftM PostEventRequest  --}

toRecordedEvent (PostEventRequest sId v d) = RecordedEvent {
  recordedEventStreamId = sId,
  recordedEventNumber = v,
  recordedEventData = d }

-- todo: this doesn't actually run a simulation yet
runActions :: [PostEventRequest] -> Gen [RecordedEvent]
runActions a = elements $  [fmap toRecordedEvent a]

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
          [ testCase "Can write event" $
            let
              (_,s) = runState (runTest sampleWrite) M.empty
              expected = M.singleton testKey ("FooCreatedEvent", BS.empty, Nothing)
            in
              assertEqual "Event is in the map" expected s
          ],
        postEventSpec',
        webserverInternalTests',
        testProperty "Test QuickCheck Property" prop_List,
        testProperty "All Events Appear in Subscription" prop_AllEventsAppearInSubscription
      ]
