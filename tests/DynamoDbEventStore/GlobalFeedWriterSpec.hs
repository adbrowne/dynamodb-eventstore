{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE FlexibleContexts #-}

module DynamoDbEventStore.GlobalFeedWriterSpec (tests) where

import           BasicPrelude
import           Control.Lens
import           Test.Tasty
import           Test.Tasty.QuickCheck((===),testProperty)
import qualified Test.Tasty.QuickCheck as QC
import           Test.Tasty.HUnit
import           Control.Monad.State
import           Control.Monad.Loops
import qualified Data.Text             as T
import qualified Data.Text.Encoding             as T
import qualified Data.Text.Lazy        as TL
import qualified Data.Text.Lazy.Encoding    as TL
import qualified Data.Map.Strict as Map
import qualified Data.Sequence as Seq
import qualified Data.Aeson as Aeson
import qualified Data.Set as Set

import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.EventStoreActions
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           DynamoDbEventStore.GlobalFeedWriter (FeedEntry())
import           DynamoDbEventStore.DynamoCmdInterpreter

type UploadItem = (Text,Int64,[EventData])
newtype UploadList = UploadList [UploadItem] deriving (Show)

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen [a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

newtype EventData = EventData Text deriving (Eq,Show)
instance QC.Arbitrary EventData where
  arbitrary = EventData . T.pack <$> QC.arbitrary
  shrink (EventData xs) = EventData . T.pack <$> QC.shrink (T.unpack xs)

uniqueList :: Ord a => [a] -> [a]
uniqueList = Set.toList . Set.fromList

instance QC.Arbitrary UploadList where
  arbitrary = do
    (streams :: [StreamId]) <- uniqueList <$> cappedList 5
    (streamWithEvents :: [(StreamId, [[EventData]])]) <- mapM (\s -> puts >>= (\p -> return (s,p))) streams
    return $ UploadList $ numberedPuts streamWithEvents 
    where
      numberedPuts :: [(StreamId, [[EventData]])] -> [(Text, Int64, [EventData])]
      numberedPuts xs = (\(StreamId s,d) -> banana s d) =<< xs
      banana :: Text -> [[EventData]] -> [(Text,Int64, [EventData])]
      banana streamId xs = reverse $ evalState (foldM (apple streamId) [] xs) (-1)
      apple :: Text -> [(Text,Int64, [EventData])] -> [EventData] -> State Int64 [(Text,Int64, [EventData])]
      apple streamId xs x = do
        (eventNumber :: Int64) <- get
        put (eventNumber + (fromIntegral . length) x)
        return $ (streamId, eventNumber, x):xs
      puts :: QC.Gen [[EventData]]
      puts = do 
        (p :: [()]) <- cappedList 100
        mapM (\_ -> cappedList 10) p

writeEvent :: (Text, Int64, [EventData]) -> DynamoCmdM (Either Text EventWriteResult)
writeEvent (stream, eventNumber, eventBodies) = 
  let 
    eventEntries = (\(EventData body) -> EventEntry (TL.encodeUtf8 . TL.fromStrict $ body) "") <$> eventBodies
  in
    postEventRequestProgram (PostEventRequest stream (Just eventNumber) eventEntries)

publisher :: [(Text,Int64,[EventData])] -> DynamoCmdM (Either Text ())
publisher xs = Right <$> forM_ xs writeEvent

globalFeedFromUploadList :: [UploadItem] -> Map.Map Text (Seq.Seq Int64)
globalFeedFromUploadList =
  foldl' acc Map.empty
  where
    acc :: Map.Map Text (Seq.Seq Int64) -> UploadItem -> Map.Map Text (Seq.Seq Int64)
    acc s (stream, expectedVersion, bodies) =
      let 
        eventVersions = Seq.fromList $ take (length bodies) [expectedVersion + 1..]
        newValue = maybe eventVersions (Seq.>< eventVersions) $ Map.lookup stream s
      in Map.insert stream newValue s

globalRecordedEventListToMap :: [EventKey] -> Map.Map Text (Seq.Seq Int64)
globalRecordedEventListToMap = 
  foldl' acc Map.empty
  where
    acc :: Map.Map Text (Seq.Seq Int64) -> EventKey -> Map.Map Text (Seq.Seq Int64)
    acc s (EventKey (StreamId stream, number)) =
      let newValue = maybe (Seq.singleton number) (Seq.|> number) $ Map.lookup stream s
      in Map.insert stream newValue s

prop_EventShouldAppearInGlobalFeedInStreamOrder :: UploadList -> QC.Property
prop_EventShouldAppearInGlobalFeedInStreamOrder (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100))
      , ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100))
      , ("GlobalFeedWriter2", (GlobalFeedWriter.main, 100)) 
      ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = QC.forAll (runReadAllProgram testState) (\feedItems -> (globalRecordedEventListToMap <$> feedItems) === (Right $ globalFeedFromUploadList uploadList))
       runReadAllProgram = runProgramGenerator "readAllRequestProgram" (getReadAllRequestProgram ReadAllRequest)

expectedEventsFromUploadList :: UploadList -> [(Text, Int64, EventData)]
expectedEventsFromUploadList (UploadList uploadItems) = do
  (streamId, firstEventNumber, bodies) <- uploadItems
  (eventNumber, eventData) <- zip [firstEventNumber+1..] bodies
  return (streamId, eventNumber, eventData)

prop_AllEventsCanBeReadIndividually :: UploadList -> QC.Property
prop_AllEventsCanBeReadIndividually (UploadList uploadItems) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadItems,100))
      ]
    expectedEvents = expectedEventsFromUploadList (UploadList uploadItems)
    check (_, testState) = lookupBodies testState === ((_3 %~ Just) <$> expectedEvents)
    lookupBodies :: TestState -> [(Text, Int64, Maybe EventData)]
    lookupBodies testState = fmap (\(streamId, eventNumber, _) -> (streamId, eventNumber, lookupBody testState streamId eventNumber)) expectedEvents
    lookupBody :: TestState ->  Text -> Int64 -> (Maybe EventData)
    lookupBody testState streamId eventNumber =
      (EventData . T.decodeUtf8 . recordedEventData) <$> evalProgram "LookupEvent" (getReadEventRequestProgram $ ReadEventRequest streamId eventNumber) testState
  in QC.forAll (runPrograms programs) check

prop_ConflictingWritesWillNotSucceed :: QC.Property
prop_ConflictingWritesWillNotSucceed =
  let
    programs = Map.fromList [
        ("WriteOne", (writeEvent ("MyStream",-1,[EventData "", EventData ""]), 10))
      , ("WriteTwo", (writeEvent ("MyStream",-1,[EventData ""]), 10))
      ]
  in QC.forAll (runPrograms programs) check
     where
       check (writeResults, _testState) = (foldl' sumIfSuccess 0 writeResults) === 1
       sumIfSuccess :: Int -> Either Text EventWriteResult -> Int
       sumIfSuccess s (Right WriteSuccess) = s + 1
       sumIfSuccess s _            = s

getStreamRecordedEvents :: Text -> DynamoCmdM [RecordedEvent]
getStreamRecordedEvents streamId = do
   recordedEvents <- concat <$> unfoldrM getEventSet Nothing 
   return $ reverse recordedEvents
   where
    getEventSet :: Maybe Int64 -> DynamoCmdM (Maybe ([RecordedEvent], Maybe Int64)) 
    getEventSet startEvent = 
      if ((< 0) <$> startEvent) == Just True then
        return Nothing
      else do
        recordedEvents <- getReadStreamRequestProgram (ReadStreamRequest streamId startEvent)
        if null recordedEvents then
          return Nothing
        else 
          return $ Just (recordedEvents, (Just . (\x -> x - 1) . recordedEventNumber . last) recordedEvents)

readEachStream :: [UploadItem] -> DynamoCmdM (Map.Map Text (Seq.Seq Int64))
readEachStream uploadItems = 
  foldM readStream Map.empty streams
  where 
    readStream :: Map.Map Text (Seq.Seq Int64) -> Text -> DynamoCmdM (Map.Map Text (Seq.Seq Int64))
    readStream m streamId = do
      eventIds <- getEventIds streamId
      return $ Map.insert streamId eventIds m
    getEventIds :: Text -> DynamoCmdM (Seq.Seq Int64)
    getEventIds streamId = do
       recordedEvents <- getStreamRecordedEvents streamId
       return $ Seq.fromList $ recordedEventNumber <$> recordedEvents
    streams :: [Text]
    streams = (\(stream, _, _) -> stream) <$> uploadItems

prop_EventsShouldAppearInTheirSteamsInOrder :: UploadList -> QC.Property
prop_EventsShouldAppearInTheirSteamsInOrder (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)), 
      ("GlobalFeedWriter2", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = runReadEachStream testState === globalFeedFromUploadList uploadList
       runReadEachStream = evalProgram "readEachStream" (readEachStream uploadList)

prop_ScanUnpagedShouldBeEmpty :: UploadList -> QC.Property
prop_ScanUnpagedShouldBeEmpty (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)), 
      ("GlobalFeedWriter2", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = scanUnpaged testState === []
       scanUnpaged = evalProgram "scanUnpaged" scanNeedsPaging'

type EventWriter = StreamId -> [(Text, LByteString)] -> DynamoCmdM ()

writeEventsWithExplicitExpectedVersions :: EventWriter
writeEventsWithExplicitExpectedVersions (StreamId streamId) events =
  evalStateT (forM_ events writeSingleEvent) (-1)
  where 
    writeSingleEvent (et, ed) = do
      eventNumber <- get
      result <- lift $ postEventRequestProgram (PostEventRequest streamId (Just eventNumber) [EventEntry ed (EventType et)])
      when (result /= Right WriteSuccess) $ error "Bad write result"
      put (eventNumber + 1)

writeEventsWithNoExpectedVersions :: EventWriter
writeEventsWithNoExpectedVersions (StreamId streamId) events =
  forM_ events writeSingleEvent
  where 
    writeSingleEvent (et, ed) = do
      result <- postEventRequestProgram (PostEventRequest streamId Nothing [EventEntry ed (EventType et)])
      when (result /= Right WriteSuccess) $ error "Bad write result"

writeThenRead :: StreamId -> [(Text, LByteString)] -> EventWriter -> DynamoCmdM [RecordedEvent]
writeThenRead (StreamId streamId) events writer = do
  writer (StreamId streamId) events
  getStreamRecordedEvents streamId
  
writtenEventsAppearInReadStream :: EventWriter -> Assertion
writtenEventsAppearInReadStream writer = 
  let 
    streamId = StreamId "MyStream"
    eventDatas = [("MyEvent", TL.encodeUtf8 "My Content"), ("MyEvent2", TL.encodeUtf8 "My Content2")]
    expectedResult = [
      RecordedEvent { 
        recordedEventStreamId = "MyStream", 
        recordedEventNumber = 0, 
        recordedEventData = T.encodeUtf8 "My Content", 
        recordedEventType = "MyEvent"
      }, 
      RecordedEvent { 
        recordedEventStreamId = "MyStream", 
        recordedEventNumber = 1, 
        recordedEventData = T.encodeUtf8 "My Content2", 
        recordedEventType = "MyEvent2"} ] 
    result = evalProgram "writeThenRead" (writeThenRead streamId eventDatas writer) emptyTestState
  in assertEqual "Returned events should match input events" expectedResult result

prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter :: [PostEventRequest] -> QC.Property
prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter events =
  let
    programs = Map.fromList [
      ("Publisher", (Right <$> forM_ events postEventRequestProgram, 100))
      , ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100))
      ]
  in QC.forAll (runPrograms programs) check
     where
       -- global feed writer runs forever unless there is an error so we don't
       -- expect a result
       check (results, _) = Map.lookup "GlobalFeedWriter1" results === Nothing 

cannotWriteEventsOutOfOrder :: Assertion
cannotWriteEventsOutOfOrder =
  let 
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just 1, perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent"] }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return an error" (Right WrongExpectedVersion) result

canWriteFirstEvent :: Assertion
canWriteFirstEvent =
  let 
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just (-1), perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent"] }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

canWriteMultipleEvents :: Assertion
canWriteMultipleEvents =
  let 
    multiPostEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just (-1), perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent", EventEntry (TL.encodeUtf8 "My Content2") "MyEvent2"] }
    subsequentPostEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just 1, perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent"] }
    result = evalProgram "writeEvent" (postEventRequestProgram multiPostEventRequest >> postEventRequestProgram subsequentPostEventRequest) emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

eventNumbersCorrectForMultipleEvents :: Assertion
eventNumbersCorrectForMultipleEvents =
  let 
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent", EventEntry (TL.encodeUtf8 "My Content2") "MyEvent2"] }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just 1, perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent"] }
    result = evalProgram "writeEvent" (postEventRequestProgram multiPostEventRequest >> postEventRequestProgram subsequentPostEventRequest >> getStreamRecordedEvents streamId) emptyTestState
    eventNumbers = recordedEventNumber <$> result
  in assertEqual "Should return success" [0,1,2] eventNumbers

errorThrownIfTryingToWriteAnEventInAMultipleGap :: Assertion
errorThrownIfTryingToWriteAnEventInAMultipleGap =
  let 
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent", EventEntry (TL.encodeUtf8 "My Content2") "MyEvent2"] }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = [EventEntry (TL.encodeUtf8 "My Content") "MyEvent"] }
    result = evalProgram "writeEvents" (postEventRequestProgram multiPostEventRequest >> postEventRequestProgram subsequentPostEventRequest) emptyTestState
  in assertEqual "Should return failure" (Right EventExists) result

tests :: [TestTree]
tests = [
      testProperty "Can round trip FeedEntry via JSON" (\(a :: FeedEntry) -> (Aeson.decode . Aeson.encode) a === Just a),
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder,
      testProperty "Each event appears in it's correct stream" prop_EventsShouldAppearInTheirSteamsInOrder,
      testProperty "No Write Request can cause a fatal error in global feed writer" prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter,
      testProperty "Conflicting writes will not succeed" prop_ConflictingWritesWillNotSucceed,
      testProperty "All Events can be read individually" prop_AllEventsCanBeReadIndividually,
      testProperty "Scan unpaged should be empty" prop_ScanUnpagedShouldBeEmpty,
      --testProperty "The result of multiple writers matches what they see" todo,
      --testProperty "Get stream items contains event lists without duplicates or gaps" todo,
      testCase "Unit - Written Events Appear In Read Stream - explicit expected version" $ writtenEventsAppearInReadStream writeEventsWithExplicitExpectedVersions,
      testCase "Unit - Written Events Appear In Read Stream - explicit expected version" $ writtenEventsAppearInReadStream writeEventsWithNoExpectedVersions,
      testCase "Unit - Cannot write event if previous one does not exist" cannotWriteEventsOutOfOrder,
      testCase "Unit - Can write first event" canWriteFirstEvent,
      testCase "Unit - Can write multiple events" canWriteMultipleEvents,
      testCase "Unit - Error thrown if trying to write an event in a multiple gap" errorThrownIfTryingToWriteAnEventInAMultipleGap,
      testCase "Unit - EventNumbers are calculated when there are multiple events" eventNumbersCorrectForMultipleEvents
  ]
