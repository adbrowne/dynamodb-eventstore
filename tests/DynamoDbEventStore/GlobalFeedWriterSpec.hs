{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE FlexibleContexts #-}

module DynamoDbEventStore.GlobalFeedWriterSpec where

import           Data.List
import           Data.Int
import           Test.Tasty
import           Test.Tasty.QuickCheck((===),testProperty)
import qualified Test.Tasty.QuickCheck as QC
import           Control.Monad.State
import qualified Control.Monad.Free.Church as Church
import qualified Data.HashMap.Lazy as HM
import qualified Data.Text             as T
import qualified Data.Text.Lazy        as TL
import qualified Data.Text.Lazy.Encoding    as TL
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import qualified Data.Aeson as Aeson

import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.EventStoreActions
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           DynamoDbEventStore.GlobalFeedWriter (FeedEntry())
import           DynamoDbEventStore.DynamoCmdInterpreter

type UploadItem = (T.Text,Int64,T.Text)
newtype UploadList = UploadList [UploadItem] deriving (Show)

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen [a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

newtype EventData = EventData T.Text
instance QC.Arbitrary EventData where
  arbitrary = EventData . T.pack <$> QC.arbitrary
  shrink (EventData xs) = EventData . T.pack <$> QC.shrink (T.unpack xs)

instance QC.Arbitrary UploadList where
  arbitrary = do
    streams <- cappedList 5
    events <- cappedList 100
    eventsWithStream <- mapM (assignStream streams) events
    return $ UploadList $ numberEvents eventsWithStream
    where
      assignStream streams event = do
        stream <- QC.elements streams
        return (stream, event)
      groupTuple xs = HM.toList $ HM.fromListWith (++) ((\(a,b) -> (a,[b])) <$> xs)
      numberEvents :: [(StreamId, EventData)] -> [UploadItem]
      numberEvents xs = do
        (StreamId stream, events) <- groupTuple xs
        (EventData event, number) <- zip events [0..]
        return (stream, number, event)

writeEvent :: (T.Text, Int64, T.Text) -> DynamoCmdM EventWriteResult
writeEvent (stream, eventNumber, body) = postEventRequestProgram (PostEventRequest stream eventNumber (TL.encodeUtf8 . TL.fromStrict $ body) "")

publisher :: [(T.Text,Int64,T.Text)] -> DynamoCmdM ()
publisher xs = forM_ xs writeEvent

globalFeedFromUploadList :: [UploadItem] -> Map.Map T.Text (V.Vector Int64)
globalFeedFromUploadList =
  foldl' acc Map.empty
  where
    acc :: Map.Map T.Text (V.Vector Int64) -> UploadItem -> Map.Map T.Text (V.Vector Int64)
    acc s (stream, number, _) =
      let newValue = maybe (V.singleton number) (`V.snoc` number) $ Map.lookup stream s
      in Map.insert stream newValue s

globalRecordedEventListToMap :: [EventKey] -> Map.Map T.Text (V.Vector Int64)
globalRecordedEventListToMap = 
  foldl' acc Map.empty
  where
    acc :: Map.Map T.Text (V.Vector Int64) -> EventKey -> Map.Map T.Text (V.Vector Int64)
    acc s (EventKey (StreamId stream, number)) =
      let newValue = maybe (V.singleton number) (`V.snoc` number) $ Map.lookup stream s
      in Map.insert stream newValue s

prop_EventShouldAppearInGlobalFeedInStreamOrder :: UploadList -> QC.Property
prop_EventShouldAppearInGlobalFeedInStreamOrder (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = QC.forAll (runReadAllProgram testState) (\feedItems -> (globalRecordedEventListToMap <$> feedItems) === (Just $ globalFeedFromUploadList uploadList))
       runReadAllProgram = runProgram "readAllRequestProgram" (Church.fromF (getReadAllRequestProgram ReadAllRequest))

readEachStream :: [UploadItem] -> DynamoCmdM (Map.Map T.Text (V.Vector Int64))
readEachStream uploadItems = 
  foldM readStream Map.empty streams
  where 
    readStream :: Map.Map T.Text (V.Vector Int64) -> T.Text -> DynamoCmdM (Map.Map T.Text (V.Vector Int64))
    readStream m streamId = do
      eventIds <- getEventIds streamId
      return $ Map.insert streamId eventIds m
    getEventIds :: T.Text -> DynamoCmdM (V.Vector Int64)
    getEventIds streamId = do
       recordedEvents <- getReadStreamRequestProgram (ReadStreamRequest streamId)
       return $ V.fromList $ recordedEventNumber <$> reverse recordedEvents
    streams :: [T.Text]
    streams = (\(stream, _, _) -> stream) <$> uploadItems

prop_EventsShouldAppearInTheirSteamsInOrder :: UploadList -> QC.Property
prop_EventsShouldAppearInTheirSteamsInOrder (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = QC.forAll (runReadEachStream testState) (\streamItems -> streamItems === (Just $ globalFeedFromUploadList uploadList))
       runReadEachStream = runProgram "readEachStream" (Church.fromF (readEachStream uploadList))

tests :: [TestTree]
tests = [
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder,
      testProperty "Each event appears in it's correct stream" prop_EventsShouldAppearInTheirSteamsInOrder,
      testProperty "Can round trip FeedEntry via JSON" (\(a :: FeedEntry) -> (Aeson.decode . Aeson.encode) a === Just a)
  ]
