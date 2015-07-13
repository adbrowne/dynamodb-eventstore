{-# LANGUAGE OverloadedStrings #-}

module DynamoDbEventStore.EventStoreActionTests (tests) where

import           Control.Monad.State
import           Control.Monad
import qualified Data.ByteString.Lazy       as BL
import qualified Data.List                  as L
import qualified Data.Map                   as M
import qualified Data.Set                   as S
import qualified Data.Text                  as T
import           DynamoDbEventStore.Testing
import           EventStoreActions
import           EventStoreCommands
import           Test.Tasty
import           Test.Tasty.QuickCheck

import qualified Test.QuickCheck.Gen        as QC
import qualified Test.QuickCheck.Random     as QC

runItem :: FakeState -> PostEventRequest -> Gen FakeState
runItem fakeState (PostEventRequest sId v d et) = do
  (_,s) <- runStateT (runTestGen writeItem) fakeState
  return s
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

getEventsFromState s = do
  (_, pagedKeys) <- (M.elems . snd) s
  pagedKeys

runActions :: [PostEventRequest] -> Gen FakeState
runActions a = do
  s <- foldM runItem emptyTestState a
  (_,s') <- runStateT (runTestGen (writePagesProgram $ Just 100)) s
  return s'

prop_AllEventsAppearInSubscription :: SingleStreamValidActions -> Property
prop_AllEventsAppearInSubscription (SingleStreamValidActions actions) =
  forAll (runActions actions) $ \s ->
    S.fromList (getEventsFromState s) === S.fromList (map toEventKey actions)

prop_GlobalFeedPreservesEventOrdering :: SingleStreamValidActions -> Property
prop_GlobalFeedPreservesEventOrdering (SingleStreamValidActions actions) =
  forAll (runActions actions) $ \s ->
    whenFail
       (putStrLn $ show s) 
       (getEventsFromState s === map toEventKey actions)

tests :: [TestTree]
tests = [
      testProperty "All Events Appear in Subscription" prop_AllEventsAppearInSubscription
      , testProperty "Global Feed preserves stream oder" prop_GlobalFeedPreservesEventOrdering
  ]
