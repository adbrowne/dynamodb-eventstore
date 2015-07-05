{-# LANGUAGE OverloadedStrings #-}

module DynamoDbEventStore.EventStoreActionTests (tests) where

import           Control.Monad.State
import qualified Data.Map                as M
import qualified Data.List               as L
import qualified Data.Set                as S
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text               as T
import           DynamoDbEventStore.Testing
import           EventStoreActions
import           EventStoreCommands
import           Test.Tasty
import           Test.Tasty.QuickCheck

import qualified Test.QuickCheck.Gen as QC
import qualified Test.QuickCheck.Random as QC
import qualified System.Random as Random
newtype GenT m a = GenT { unGenT :: QC.QCGen -> Int -> m a }

runGenT :: GenT m a -> QC.Gen (m a)
runGenT (GenT run) = QC.MkGen run

runItem :: FakeState -> PostEventRequest -> FakeState
runItem fakeState (PostEventRequest sId v d et) =
  let
     (_,s) = runState (runTest writeItem) fakeState
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

toEventKey :: PostEventRequest -> EventKey
toEventKey (PostEventRequest sId v _ _) =
  EventKey (StreamId sId, v)

runActions :: [PostEventRequest] -> Gen ([EventKey])
runActions a = do
  let s = L.foldl' runItem emptyTestState a
  (_,s') <- runStateT (runTest (writePagesProgram $ Just 100)) s
  return $ events s'
  where
    events s = do
      (_, pagedKeys) <- (M.elems . snd) s
      pagedKeys

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
