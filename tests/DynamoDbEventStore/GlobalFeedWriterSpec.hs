{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE RecordWildCards #-}
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
import           Control.Monad.Except
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Either.Combinators
import           Data.Time.Clock
import           Data.Time.Calendar
import           Data.Time.Format
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
import           DynamoDbEventStore.GlobalFeedWriter (FeedEntry(),EventStoreActionError(..))
import           DynamoDbEventStore.DynamoCmdInterpreter

type ExtraEventFields = (UTCTime, Bool)
type UploadItem = (Text,Int64,ExtraEventFields,NonEmpty EventData)
newtype UploadList = UploadList [UploadItem] deriving (Show)

sampleTime :: UTCTime
sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000"

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen [a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

newtype EventData = EventData Text deriving (Eq,Show)
instance QC.Arbitrary EventData where
  arbitrary = EventData . T.pack <$> QC.arbitrary
  shrink (EventData xs) = EventData . T.pack <$> QC.shrink (T.unpack xs)

uniqueList :: Ord a => [a] -> [a]
uniqueList = Set.toList . Set.fromList

newtype SecondPrecisionUtcTime = SecondPrecisionUtcTime UTCTime

instance QC.Arbitrary SecondPrecisionUtcTime where
  arbitrary =
    SecondPrecisionUtcTime <$> (UTCTime
     <$> (QC.arbitrary  :: QC.Gen Day)
     <*> (secondsToDiffTime <$> QC.choose (0, 86400)))
     
instance QC.Arbitrary UploadList where
  arbitrary = do
    (streams :: [StreamId]) <- uniqueList <$> cappedList 5
    (streamWithEvents :: [(StreamId, [NonEmpty EventData])]) <- mapM (\s -> puts >>= (\p -> return (s,p))) streams
    (withTime :: [(StreamId, [NonEmpty EventData], ExtraEventFields)]) <- mapM (\(s,es) -> generateExtraFields >>= (\t -> return (s,es,t))) streamWithEvents
    return $ UploadList $ numberedPuts withTime 
    where
      generateExtraFields :: QC.Gen ExtraEventFields
      generateExtraFields =
        (,) 
        <$> ((\(SecondPrecisionUtcTime x) -> x) <$> QC.arbitrary)
        <*> QC.arbitrary
      numberedPuts :: [(StreamId, [NonEmpty EventData], ExtraEventFields)] -> [(Text, Int64, ExtraEventFields, NonEmpty EventData)]
      numberedPuts xs = (\(StreamId s,d,t) -> banana s d t) =<< xs
      banana :: Text -> [NonEmpty EventData] -> ExtraEventFields -> [(Text,Int64, ExtraEventFields, NonEmpty EventData)]
      banana streamId xs t = reverse $ evalState (foldM (apple streamId t) [] xs) (-1)
      apple :: Text -> ExtraEventFields -> [(Text,Int64, ExtraEventFields, NonEmpty EventData)] -> NonEmpty EventData -> State Int64 [(Text,Int64, ExtraEventFields, NonEmpty EventData)]
      apple streamId extra xs x = do
        (eventNumber :: Int64) <- get
        put (eventNumber + (fromIntegral . length) x)
        return $ (streamId, eventNumber, extra, x):xs
      puts :: QC.Gen [NonEmpty EventData]
      puts = do 
        (p :: [()]) <- cappedList 100
        mapM (\_ -> (:|) <$> QC.arbitrary <*> cappedList 9) p

writeEvent :: (Text, Int64, ExtraEventFields, NonEmpty EventData) -> DynamoCmdM (Either EventStoreActionError EventWriteResult)
writeEvent (stream, eventNumber, (eventTime, isJson), eventBodies) = 
  let 
    eventEntries = (\(EventData body) -> EventEntry (TL.encodeUtf8 . TL.fromStrict $ body) "") <$> eventBodies
  in
    postEventRequestProgram (PostEventRequest stream (Just eventNumber) eventTime isJson eventEntries)

publisher :: [(Text,Int64,ExtraEventFields,NonEmpty EventData)] -> DynamoCmdM (Either EventStoreActionError ())
publisher xs = Right <$> forM_ xs writeEvent

globalFeedFromUploadList :: [UploadItem] -> Map.Map Text (Seq.Seq Int64)
globalFeedFromUploadList =
  foldl' acc Map.empty
  where
    acc :: Map.Map Text (Seq.Seq Int64) -> UploadItem -> Map.Map Text (Seq.Seq Int64)
    acc s (stream, expectedVersion, _eventTime, bodies) =
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
       check (_, testRunState) = QC.forAll (runReadAllProgram testRunState) (\feedItems -> (globalRecordedEventListToMap <$> feedItems) === (Right $ globalFeedFromUploadList uploadList))
       runReadAllProgram = runProgramGenerator "readAllRequestProgram" (getReadAllRequestProgram ReadAllRequest)

expectedEventsFromUploadList :: UploadList -> [RecordedEvent]
expectedEventsFromUploadList (UploadList uploadItems) = do
  (streamId, firstEventNumber, (eventTime, isJson), bodies) <- uploadItems
  (eventNumber, EventData eventData) <- zip [firstEventNumber+1..] (NonEmpty.toList bodies)
  return $ RecordedEvent { 
    recordedEventStreamId = streamId,
    recordedEventNumber = eventNumber,
    recordedEventData = T.encodeUtf8 eventData,
    recordedEventType = "",
    recordedEventCreated = eventTime,
    recordedEventIsJson = isJson }

prop_AllEventsCanBeReadIndividually :: UploadList -> QC.Property
prop_AllEventsCanBeReadIndividually (UploadList uploadItems) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadItems,100))
      ]
    expectedEvents = expectedEventsFromUploadList (UploadList uploadItems)
    check (_, testRunState) = lookupBodies testRunState === ((Right . Just) <$> expectedEvents)
    lookupBodies testRunState = fmap (\RecordedEvent{..} -> (lookupBody testRunState recordedEventStreamId recordedEventNumber)) expectedEvents
    lookupBody testRunState streamId eventNumber =
      (id <$>) <$> evalProgram "LookupEvent" (getReadEventRequestProgram $ ReadEventRequest streamId eventNumber) testRunState
  in QC.forAll (runPrograms programs) check

prop_ConflictingWritesWillNotSucceed :: QC.Property
prop_ConflictingWritesWillNotSucceed =
  let
    programs = Map.fromList [
        ("WriteOne", (writeEvent ("MyStream",-1, (sampleTime, False), EventData "" :| [EventData ""]), 10))
      , ("WriteTwo", (writeEvent ("MyStream",-1, (sampleTime, False), EventData "" :| []), 10))
      ]
  in QC.forAll (runPrograms programs) check
     where
       check (writeResults, _testState) = (foldl' sumIfSuccess 0 writeResults) === 1
       sumIfSuccess :: Int -> Either e EventWriteResult -> Int
       sumIfSuccess s (Right WriteSuccess) = s + 1
       sumIfSuccess s _            = s

getStreamRecordedEvents :: Text -> ExceptT EventStoreActionError DynamoCmdM [RecordedEvent]
getStreamRecordedEvents streamId = do
   recordedEvents <- concat <$> unfoldrM getEventSet Nothing 
   return $ reverse recordedEvents
   where
    getEventSet :: Maybe Int64 -> ExceptT EventStoreActionError DynamoCmdM (Maybe ([RecordedEvent], Maybe Int64)) 
    getEventSet startEvent = 
      if ((< 0) <$> startEvent) == Just True then
        return Nothing
      else do
        recordedEvents <- (lift $ getReadStreamRequestProgram (ReadStreamRequest streamId startEvent)) >>= eitherToError
        if null recordedEvents then
          return Nothing
        else 
          return $ Just (recordedEvents, (Just . (\x -> x - 1) . recordedEventNumber . last) recordedEvents)

readEachStream :: [UploadItem] -> ExceptT EventStoreActionError DynamoCmdM (Map.Map Text (Seq.Seq Int64))
readEachStream uploadItems = 
  foldM readStream Map.empty streams
  where 
    readStream :: Map.Map Text (Seq.Seq Int64) -> Text -> ExceptT EventStoreActionError DynamoCmdM (Map.Map Text (Seq.Seq Int64))
    readStream m streamId = do
      eventIds <- getEventIds streamId
      return $ Map.insert streamId eventIds m
    getEventIds :: Text -> ExceptT EventStoreActionError DynamoCmdM (Seq.Seq Int64)
    getEventIds streamId = do
       recordedEvents <- getStreamRecordedEvents streamId
       return $ Seq.fromList $ recordedEventNumber <$> recordedEvents
    streams :: [Text]
    streams = (\(stream, _, _, _) -> stream) <$> uploadItems

prop_EventsShouldAppearInTheirSteamsInOrder :: UploadList -> QC.Property
prop_EventsShouldAppearInTheirSteamsInOrder (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)), 
      ("GlobalFeedWriter2", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testRunState) = runReadEachStream testRunState === (Right $ globalFeedFromUploadList uploadList)
       runReadEachStream = evalProgram "readEachStream" (runExceptT (readEachStream uploadList))

prop_ScanUnpagedShouldBeEmpty :: UploadList -> QC.Property
prop_ScanUnpagedShouldBeEmpty (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)), 
      ("GlobalFeedWriter2", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testRunState) = scanUnpaged testRunState === []
       scanUnpaged = evalProgram "scanUnpaged" scanNeedsPaging'

type EventWriter = StreamId -> [(Text, LByteString)] -> DynamoCmdM ()

writeEventsWithExplicitExpectedVersions :: EventWriter
writeEventsWithExplicitExpectedVersions (StreamId streamId) events =
  evalStateT (forM_ events writeSingleEvent) (-1)
  where 
    writeSingleEvent (et, ed) = do
      eventNumber <- get
      result <- lift $ postEventRequestProgram (PostEventRequest streamId (Just eventNumber) sampleTime False (EventEntry ed (EventType et) :| []))
      when (result /= Right WriteSuccess) $ error "Bad write result"
      put (eventNumber + 1)

writeEventsWithNoExpectedVersions :: EventWriter
writeEventsWithNoExpectedVersions (StreamId streamId) events =
  forM_ events writeSingleEvent
  where 
    writeSingleEvent (et, ed) = do
      result <- postEventRequestProgram (PostEventRequest streamId Nothing sampleTime False (EventEntry ed (EventType et) :| []))
      when (result /= Right WriteSuccess) $ error "Bad write result"

writeThenRead :: StreamId -> [(Text, LByteString)] -> EventWriter -> ExceptT EventStoreActionError DynamoCmdM [RecordedEvent]
writeThenRead (StreamId streamId) events writer = do
  lift $ writer (StreamId streamId) events
  getStreamRecordedEvents streamId
  
writtenEventsAppearInReadStream :: EventWriter -> Assertion
writtenEventsAppearInReadStream writer = 
  let 
    streamId = StreamId "MyStream"
    eventDatas = [("MyEvent", TL.encodeUtf8 "My Content"), ("MyEvent2", TL.encodeUtf8 "My Content2")]
    expectedResult = Right [
      RecordedEvent { 
        recordedEventStreamId = "MyStream", 
        recordedEventNumber = 0, 
        recordedEventData = T.encodeUtf8 "My Content", 
        recordedEventType = "MyEvent",
        recordedEventCreated = sampleTime,
        recordedEventIsJson = False
      }, 
      RecordedEvent { 
        recordedEventStreamId = "MyStream", 
        recordedEventNumber = 1, 
        recordedEventData = T.encodeUtf8 "My Content2", 
        recordedEventType = "MyEvent2",
        recordedEventCreated = sampleTime,
        recordedEventIsJson = False
      } ] 
    result = evalProgram "writeThenRead" (runExceptT $ writeThenRead streamId eventDatas writer) emptyTestState
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
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just 1, perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| []) }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return an error" (Right WrongExpectedVersion) result

canWriteFirstEvent :: Assertion
canWriteFirstEvent =
  let 
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just (-1), perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| []) }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

canWriteMultipleEvents :: Assertion
canWriteMultipleEvents =
  let 
    multiPostEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just (-1), perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| [EventEntry (TL.encodeUtf8 "My Content2") "MyEvent2"]) }
    subsequentPostEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just 1, perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| []) }
    result = evalProgram "writeEvent" (postEventRequestProgram multiPostEventRequest >> postEventRequestProgram subsequentPostEventRequest) emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

eventNumbersCorrectForMultipleEvents :: Assertion
eventNumbersCorrectForMultipleEvents =
  let 
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| [EventEntry (TL.encodeUtf8 "My Content2") "MyEvent2"]) }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just 1, perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| []) }
    result = evalProgram "writeEvent" (runExceptT $ lift (postEventRequestProgram multiPostEventRequest) >> lift (postEventRequestProgram subsequentPostEventRequest) >> getStreamRecordedEvents streamId) emptyTestState
    eventNumbers = (recordedEventNumber <$>) <$> result
  in assertEqual "Should return success" (Right [0,1,2]) eventNumbers

whenIndexing1000ItemsIopsIsMinimal :: Assertion
whenIndexing1000ItemsIopsIsMinimal = 
  let 
    streamId = "MyStream"
    requests = replicate 1000 $ postEventRequestProgram $ PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| []) }
    writeState = execProgram "writeEvents" (forM_ requests id) emptyTestState 
    afterIndexState = execProgramUntilIdle "indexer" GlobalFeedWriter.main (view testState writeState)
    expectedWriteState = Map.fromList [
      ((UnpagedRead,IopsScanUnpaged,"indexer"),1000)
     ,((TableRead,IopsGetItem,"indexer"),106986)
     ,((TableRead,IopsQuery,"indexer"),999)
     ,((TableRead,IopsQuery,"writeEvents"),999)
     ,((TableWrite,IopsWrite,"indexer"),3089)
     ,((TableWrite,IopsWrite,"writeEvents"),1000)]
  in assertEqual "Should be small iops" expectedWriteState (view iopCounts afterIndexState)

errorThrownIfTryingToWriteAnEventInAMultipleGap :: Assertion
errorThrownIfTryingToWriteAnEventInAMultipleGap =
  let 
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| [EventEntry (TL.encodeUtf8 "My Content2") "MyEvent2"]) }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEventTime = sampleTime, perIsJson = False, perEvents = (EventEntry (TL.encodeUtf8 "My Content") "MyEvent" :| []) }
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
      testCase "Unit - Check Iops usage" whenIndexing1000ItemsIopsIsMinimal,
      testCase "Unit - Error thrown if trying to write an event in a multiple gap" errorThrownIfTryingToWriteAnEventInAMultipleGap,
      testCase "Unit - EventNumbers are calculated when there are multiple events" eventNumbersCorrectForMultipleEvents
  ]
