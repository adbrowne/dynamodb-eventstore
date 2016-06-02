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
import           Data.Maybe (fromJust)
import           Data.Foldable hiding (concat)
import qualified Data.UUID as UUID
import           Test.Tasty.HUnit
import           Control.Monad.State
import           Control.Monad.Loops
import           Control.Monad.Except
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Either.Combinators
import           Data.Time.Format
import qualified Data.Text             as T
import qualified Data.Text.Encoding             as T
import qualified Data.ByteString.Lazy        as BL
import qualified Data.Text.Lazy.Encoding    as TL
import qualified Data.Map.Strict as Map
import           Data.Map.Strict((!))
import qualified Data.Sequence as Seq
import qualified Data.Aeson as Aeson
import qualified Data.Set as Set
import qualified Pipes.Prelude as P
import           GHC.Natural

import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.EventStoreActions
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           DynamoDbEventStore.GlobalFeedWriter (FeedEntry(),EventStoreActionError(..))
import           DynamoDbEventStore.DynamoCmdInterpreter

type UploadItem = (Text,Int64,NonEmpty EventEntry)
newtype UploadList = UploadList [UploadItem] deriving (Show)

sampleTime :: EventTime
sampleTime = EventTime $ parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000"

eventIdFromString :: String -> EventId
eventIdFromString = EventId . fromJust . UUID.fromString

sampleEventId :: EventId
sampleEventId = eventIdFromString "c2cc10e1-57d6-4b6f-9899-38d972112d8c"

sampleEventEntry :: EventEntry
sampleEventEntry = EventEntry (TL.encodeUtf8 "My Content") "MyEvent" sampleEventId sampleTime False

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen [a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

uniqueList :: Ord a => [a] -> [a]
uniqueList = Set.toList . Set.fromList

instance QC.Arbitrary UploadList where
  arbitrary = do
    (streams :: [StreamId]) <- uniqueList <$> cappedList 5
    (streamWithEvents :: [(StreamId, [NonEmpty EventEntry])]) <- mapM (\s -> puts >>= (\p -> return (s,p))) streams
    (withTime :: [(StreamId, [NonEmpty EventEntry])]) <- mapM (\(s,es) -> return (s,es)) streamWithEvents
    return $ UploadList $ numberedPuts withTime 
    where
      numberedPuts :: [(StreamId, [NonEmpty EventEntry])] -> [(Text, Int64, NonEmpty EventEntry)]
      numberedPuts xs = (\(StreamId s,d) -> banana s d) =<< xs
      banana :: Text -> [NonEmpty EventEntry] -> [(Text, Int64, NonEmpty EventEntry)]
      banana streamId xs = reverse $ evalState (foldM (apple streamId) [] xs) (-1)
      apple :: Text -> [(Text,Int64, NonEmpty EventEntry)] -> NonEmpty EventEntry -> State Int64 [(Text,Int64, NonEmpty EventEntry)]
      apple streamId xs x = do
        (eventNumber :: Int64) <- get
        put (eventNumber + (fromIntegral . length) x)
        return $ (streamId, eventNumber, x):xs
      puts :: QC.Gen [NonEmpty EventEntry]
      puts = do 
        (p :: [()]) <- cappedList 100
        mapM (\_ -> (:|) <$> QC.arbitrary <*> cappedList 9) p

writeEvent :: (Text, Int64, NonEmpty EventEntry) -> DynamoCmdM (Either EventStoreActionError EventWriteResult)
writeEvent (stream, eventNumber, eventEntries) = 
  postEventRequestProgram (PostEventRequest stream (Just eventNumber) eventEntries)

publisher :: [(Text, Int64, NonEmpty EventEntry)] -> DynamoCmdM (Either EventStoreActionError ())
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

globalRecordedEventListToMap :: [RecordedEvent] -> Map.Map Text (Seq.Seq Int64)
globalRecordedEventListToMap = 
  foldl' acc Map.empty
  where
    acc :: Map.Map Text (Seq.Seq Int64) -> RecordedEvent -> Map.Map Text (Seq.Seq Int64)
    acc s RecordedEvent {..} =
      let newValue = maybe (Seq.singleton recordedEventNumber) (Seq.|> recordedEventNumber) $ Map.lookup recordedEventStreamId s
      in Map.insert recordedEventStreamId newValue s

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

unpositive :: QC.Positive Int -> Int
unpositive (QC.Positive x) = x

fmap2 :: (Functor f, Functor f1) => (a -> b) -> f (f1 a) -> f (f1 b)
fmap2 = fmap . fmap

fmap3 :: (Functor f, Functor f1, Functor f2) => (a -> b) -> f (f1 (f2 a)) -> f (f1 (f2 b))
fmap3 = fmap . fmap . fmap

prop_CanReadAnySectionOfAStreamForward :: UploadList -> QC.Property
prop_CanReadAnySectionOfAStreamForward (UploadList uploadList) =
  let
    writeState = execProgram "publisher" (publisher uploadList) emptyTestState 
    expectedStreamEvents = globalFeedFromUploadList uploadList
    readStreamEvents streamId startEvent maxItems = fmap3 recordedEventNumber $ fmap2 streamResultEvents $ evalProgram "ReadStream" (getReadStreamRequestProgram (ReadStreamRequest streamId startEvent maxItems FeedDirectionForward)) (view testState writeState)
    expectedEvents streamId startEvent maxItems = take (fromIntegral maxItems) $ drop (fromMaybe 0 startEvent) $ toList $ expectedStreamEvents ! streamId
    check (streamId, startEvent, maxItems) = readStreamEvents (StreamId streamId) ((fromIntegral . unpositive) <$> startEvent) maxItems === Right (Just (expectedEvents streamId (unpositive <$> startEvent) maxItems))
  in QC.forAll ((,,) <$> (QC.elements . Map.keys) expectedStreamEvents <*> QC.arbitrary <*> QC.arbitrary) check

prop_CanReadAnySectionOfAStreamBackward :: UploadList -> QC.Property
prop_CanReadAnySectionOfAStreamBackward (UploadList uploadList) =
  let
    writeState = execProgram "publisher" (publisher uploadList) emptyTestState 
    expectedStreamEvents = globalFeedFromUploadList uploadList
    readStreamEvents streamId startEvent maxItems = fmap3 recordedEventNumber $ fmap2 streamResultEvents $ evalProgram "ReadStream" (getReadStreamRequestProgram (ReadStreamRequest streamId startEvent maxItems FeedDirectionBackward)) (view testState writeState)
    expectedEvents streamId Nothing maxItems = take (fromIntegral maxItems) $ reverse $ toList $ expectedStreamEvents ! streamId
    expectedEvents streamId (Just startEvent) maxItems = takeWhile (> startEvent - fromIntegral maxItems) $ dropWhile (> startEvent) $ reverse $ toList $ expectedStreamEvents ! streamId
    check (streamId, startEvent, maxItems) = QC.counterexample (T.unpack $ show $ view testState writeState) $ readStreamEvents (StreamId streamId) ((fromIntegral . unpositive) <$> startEvent) maxItems === Right (Just (expectedEvents streamId (fromIntegral . unpositive <$> startEvent) maxItems))
  in QC.forAll ((,,) <$> (QC.elements . Map.keys) expectedStreamEvents <*> QC.arbitrary <*> QC.arbitrary) check

expectedEventsFromUploadList :: UploadList -> [RecordedEvent]
expectedEventsFromUploadList (UploadList uploadItems) = do
  (streamId, firstEventNumber, eventEntries) <- uploadItems
  (eventNumber, EventEntry eventData (EventType eventType) eventId (EventTime eventTime) isJson) <- zip [firstEventNumber+1..] (NonEmpty.toList eventEntries)
  return $ RecordedEvent { 
    recordedEventStreamId = streamId,
    recordedEventNumber = eventNumber,
    recordedEventData = BL.toStrict eventData,
    recordedEventType = eventType,
    recordedEventId = eventId,
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
        ("WriteOne", (writeEvent ("MyStream",-1, sampleEventEntry :| [sampleEventEntry]), 10))
      , ("WriteTwo", (writeEvent ("MyStream",-1, sampleEventEntry :| []), 10))
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
        streamResult <- (lift $ getReadStreamRequestProgram (ReadStreamRequest (StreamId streamId) startEvent 10 FeedDirectionBackward)) >>= eitherToError
        let result = streamResultEvents <$> streamResult
        if result == Just [] then
          return Nothing
        else 
          return $ (\recordedEvents -> (recordedEvents, (Just . (\x -> x - 1) . recordedEventNumber . last) recordedEvents)) <$> result

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
       (recordedEvents :: [RecordedEvent]) <- P.toListM $ recordedEventProducerBackward (StreamId streamId) Nothing 10
       return $ Seq.fromList . reverse $ (recordedEventNumber <$> recordedEvents)
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

type EventWriter = StreamId -> [(Text, EventId, LByteString)] -> DynamoCmdM ()

writeEventsWithExplicitExpectedVersions :: EventWriter
writeEventsWithExplicitExpectedVersions (StreamId streamId) events =
  evalStateT (forM_ events writeSingleEvent) (-1)
  where 
    writeSingleEvent (et, eventId, ed) = do
      eventNumber <- get
      result <- lift $ postEventRequestProgram (PostEventRequest streamId (Just eventNumber) (EventEntry ed (EventType et) eventId sampleTime False :| []))
      when (result /= Right WriteSuccess) $ error "Bad write result"
      put (eventNumber + 1)

writeEventsWithNoExpectedVersions :: EventWriter
writeEventsWithNoExpectedVersions (StreamId streamId) events =
  forM_ events writeSingleEvent
  where 
    writeSingleEvent (et, eventId, ed) = do
      result <- postEventRequestProgram (PostEventRequest streamId Nothing (EventEntry ed (EventType et) eventId sampleTime False :| []))
      when (result /= Right WriteSuccess) $ error "Bad write result"

writeThenRead :: StreamId -> [(Text, EventId, LByteString)] -> EventWriter -> ExceptT EventStoreActionError DynamoCmdM [RecordedEvent]
writeThenRead (StreamId streamId) events writer = do
  lift $ writer (StreamId streamId) events
  getStreamRecordedEvents streamId
  
writtenEventsAppearInReadStream :: EventWriter -> Assertion
writtenEventsAppearInReadStream writer = 
  let 
    streamId = StreamId "MyStream"
    eventId1 = eventIdFromString "f3614cb1-5707-4351-8017-2f7471845a61" 
    eventId2 = eventIdFromString "9f14fcaf-7c0a-4132-8574-483f0313d7c9"
    eventDatas = [("MyEvent", eventId1, TL.encodeUtf8 "My Content"), ("MyEvent2", eventId2, TL.encodeUtf8 "My Content2")]
    expectedResult = Right [
      RecordedEvent { 
        recordedEventStreamId = "MyStream", 
        recordedEventNumber = 0, 
        recordedEventData = T.encodeUtf8 "My Content", 
        recordedEventType = "MyEvent",
        recordedEventId = eventId1,
        recordedEventCreated = unEventTime sampleTime,
        recordedEventIsJson = False
      }, 
      RecordedEvent { 
        recordedEventStreamId = "MyStream", 
        recordedEventNumber = 1, 
        recordedEventData = T.encodeUtf8 "My Content2", 
        recordedEventType = "MyEvent2",
        recordedEventId = eventId2,
        recordedEventCreated = unEventTime sampleTime,
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
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just 1, perEvents = (sampleEventEntry :| []) }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return an error" (Right WrongExpectedVersion) result

canWriteFirstEvent :: Assertion
canWriteFirstEvent =
  let 
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just (-1), perEvents = (sampleEventEntry :| []) }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

secondSampleEventEntry :: EventEntry
secondSampleEventEntry = sampleEventEntry { eventEntryType = EventType "My Event2", eventEntryData = (TL.encodeUtf8 "My Content2")}
eventNumbersCorrectForMultipleEvents :: Assertion
eventNumbersCorrectForMultipleEvents =
  let 
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = (sampleEventEntry :| [secondSampleEventEntry]) }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just 1, perEvents = (sampleEventEntry :| []) }
    result = evalProgram "writeEvent" (runExceptT $ lift (postEventRequestProgram multiPostEventRequest) >> lift (postEventRequestProgram subsequentPostEventRequest) >> getStreamRecordedEvents streamId) emptyTestState
    eventNumbers = (recordedEventNumber <$>) <$> result
  in assertEqual "Should return success" (Right [0,1,2]) eventNumbers

sampleUUIDs :: [UUID.UUID]
sampleUUIDs = 
  let 
    (startUUID :: UUID.UUID) = read "75e52b45-f4d5-445b-8dba-d3dc9b2b34b4"
    (w0,w1,w2,w3) = UUID.toWords startUUID
    createUUID n = UUID.fromWords w0 w1 w2 (n + w3)
  in createUUID <$> [0..]

sampleEventIds :: [EventId]
sampleEventIds = EventId <$> sampleUUIDs

testStateItems :: Int -> TestState
testStateItems itemCount =
  let 
    streamId = "MyStream"
    postProgram eventId = postEventRequestProgram $ PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEvents = (sampleEventEntry { eventEntryEventId = eventId } :| []) }
    requests = take itemCount $ postProgram <$> sampleEventIds
    writeState = execProgram "writeEvents" (forM_ requests id) emptyTestState 
  in view testState writeState

getSampleItems :: (Maybe Int64) -> Natural -> FeedDirection -> Either EventStoreActionError (Maybe StreamResult)
getSampleItems startEvent maxItems direction =
  evalProgram "ReadStream" (getReadStreamRequestProgram (ReadStreamRequest (StreamId "MyStream") startEvent maxItems direction)) (testStateItems 29)

streamLinkTests :: [TestTree]
streamLinkTests =
  let 
    endOfFeedBackward = ("End of feed backward", getSampleItems Nothing 20 FeedDirectionBackward)
    middleOfFeedBackward = ("Middle of feed backward", getSampleItems (Just 26) 20 FeedDirectionBackward)
    startOfFeedBackward = ("Start of feed backward", getSampleItems (Just 1) 20 FeedDirectionBackward)
    pastEndOfFeedBackward = ("Past end of feed backward", getSampleItems (Just 100) 20 FeedDirectionBackward)
    startOfFeedForward = ("Start of feed forward", getSampleItems Nothing 20 FeedDirectionForward)
    middleOfFeedForward = ("Middle of feed forward", getSampleItems (Just 3) 20 FeedDirectionForward)
    endOfFeedForward = ("End of feed forward", getSampleItems (Just 20) 20 FeedDirectionForward)
    pastEndOfFeedForward = ("Past end of feed forward", getSampleItems (Just 100) 20 FeedDirectionForward)
    streamResultLast' = ("last", streamResultLast)
    streamResultFirst' = ("first", streamResultFirst)
    streamResultNext' = ("next", streamResultNext)
    streamResultPrevious' = ("previous", streamResultPrevious)
    linkAssert (feedResultName, feedResult) (linkName, streamLink) expectedResult = 
      testCase ("Unit - " <> feedResultName <> " - " <> linkName <> " link") $ assertEqual ("Should have " <> linkName <> " link") (Right (Just expectedResult)) (fmap2 streamLink feedResult)
  in [
      linkAssert endOfFeedBackward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert endOfFeedBackward streamResultLast' (Just (FeedDirectionForward, EventStartPosition 0, 20))
    , linkAssert endOfFeedBackward streamResultNext' (Just (FeedDirectionBackward, EventStartPosition 8, 20))
    , linkAssert endOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 29, 20))
    , linkAssert middleOfFeedBackward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert middleOfFeedBackward streamResultLast' (Just (FeedDirectionForward, EventStartPosition 0, 20))
    , linkAssert middleOfFeedBackward streamResultNext' (Just (FeedDirectionBackward, EventStartPosition 6, 20))
    , linkAssert middleOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 27, 20))
    , linkAssert startOfFeedBackward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert startOfFeedBackward streamResultLast' Nothing
    , linkAssert startOfFeedBackward streamResultNext' Nothing
    , linkAssert startOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 2, 20))
    , linkAssert pastEndOfFeedBackward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert pastEndOfFeedBackward streamResultLast' (Just (FeedDirectionForward, EventStartPosition 0, 20))
    , linkAssert pastEndOfFeedBackward streamResultNext' (Just (FeedDirectionBackward, EventStartPosition 80, 20))
    , linkAssert pastEndOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 29, 20))
    , linkAssert startOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert startOfFeedForward streamResultLast' Nothing
    , linkAssert startOfFeedForward streamResultNext' Nothing
    , linkAssert startOfFeedForward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 20, 20))
    , linkAssert middleOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert middleOfFeedForward streamResultLast' (Just (FeedDirectionForward, EventStartPosition 0, 20))
    , linkAssert middleOfFeedForward streamResultNext' (Just (FeedDirectionBackward, EventStartPosition 2, 20))
    , linkAssert middleOfFeedForward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 23, 20))
    , linkAssert endOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert endOfFeedForward streamResultLast' (Just (FeedDirectionForward, EventStartPosition 0, 20))
    , linkAssert endOfFeedForward streamResultNext' (Just (FeedDirectionBackward, EventStartPosition 19, 20))
    , linkAssert endOfFeedForward streamResultPrevious' (Just (FeedDirectionForward, EventStartPosition 29, 20))
    , linkAssert pastEndOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, EventStartHead, 20))
    , linkAssert pastEndOfFeedForward streamResultLast' (Just (FeedDirectionForward, EventStartPosition 0, 20))
    , linkAssert pastEndOfFeedForward streamResultNext' (Just (FeedDirectionBackward, EventStartPosition 99, 20))
    , linkAssert pastEndOfFeedForward streamResultPrevious' Nothing
  ]

whenIndexing1000ItemsIopsIsMinimal :: Assertion
whenIndexing1000ItemsIopsIsMinimal = 
  let 
    afterIndexState = execProgramUntilIdle "indexer" GlobalFeedWriter.main (testStateItems 1000)
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
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = (sampleEventEntry :| [secondSampleEventEntry]) }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = (sampleEventEntry :| []) }
    result = evalProgram "writeEvents" (postEventRequestProgram multiPostEventRequest >> postEventRequestProgram subsequentPostEventRequest) emptyTestState
  in assertEqual "Should return failure" (Right EventExists) result

postTwoEventWithTheSameEventId :: DynamoCmdM (Either EventStoreActionError EventWriteResult)
postTwoEventWithTheSameEventId = 
  let 
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Nothing, perEvents = (sampleEventEntry :| []) }
  in postEventRequestProgram postEventRequest >> postEventRequestProgram postEventRequest

subsequentWriteWithSameEventIdReturnsSuccess :: Assertion
subsequentWriteWithSameEventIdReturnsSuccess =
  let 
    result = evalProgram "test" postTwoEventWithTheSameEventId emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

subsequentWriteWithSameEventIdDoesNotAppendSecondEvent :: Assertion
subsequentWriteWithSameEventIdDoesNotAppendSecondEvent =
  let 
    program = postTwoEventWithTheSameEventId >> (runExceptT $ getStreamRecordedEvents "MyStream")
    result = evalProgram "test" program emptyTestState
  in assertEqual "Should return a single event" (Right 1) (length <$> result)

subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents :: Assertion
subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents =
  let 
    streamId = "MyStream"
    (eventId1:eventId2:_) = sampleEventIds
    postEvents events =  postEventRequestProgram $ PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEvents = events }
    postDoubleEvents = postEvents $ sampleEventEntry { eventEntryEventId = eventId1 } :| [ sampleEventEntry { eventEntryEventId = eventId2 } ]
    postSubsequentEvent = postEvents $ sampleEventEntry { eventEntryEventId = eventId1 } :| []
    program = 
      postDoubleEvents >>
      postSubsequentEvent >>
      (runExceptT $ getStreamRecordedEvents "MyStream")
    result = evalProgram "test" program emptyTestState
  in assertEqual "Should return the first two events" (Right 2) (length <$> result)

subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect :: Assertion
subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect =
  let 
    streamId = "MyStream"
    postEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEvents = (sampleEventEntry :| []) }
    secondPost = postEventRequest { perExpectedVersion = Just 0 }
    program = postEventRequestProgram postEventRequest >> postEventRequestProgram secondPost >> (runExceptT $ getStreamRecordedEvents streamId)
    result = evalProgram "test" program emptyTestState
  in assertEqual "Should return two events" (Right 2) (length <$> result)

tests :: [TestTree]
tests = [
      testProperty "Can round trip FeedEntry via JSON" (\(a :: FeedEntry) -> (Aeson.decode . Aeson.encode) a === Just a),
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder,
      testProperty "Each event appears in it's correct stream" prop_EventsShouldAppearInTheirSteamsInOrder,
      testProperty "No Write Request can cause a fatal error in global feed writer" prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter,
      testProperty "Conflicting writes will not succeed" prop_ConflictingWritesWillNotSucceed,
      testProperty "All Events can be read individually" prop_AllEventsCanBeReadIndividually,
      testProperty "Scan unpaged should be empty" prop_ScanUnpagedShouldBeEmpty,
      testProperty "Can read any section of a stream forward" prop_CanReadAnySectionOfAStreamForward,
      testProperty "Can read any section of a stream backward" prop_CanReadAnySectionOfAStreamBackward,
      --testProperty "The result of multiple writers matches what they see" todo,
      --testProperty "Get stream items contains event lists without duplicates or gaps" todo,
      testCase "Unit - Subsequent write with same event id returns success" subsequentWriteWithSameEventIdReturnsSuccess,
      testCase "Unit - Subsequent write with same event id does not append event - multi event" subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents,
      testCase "Unit - Subsequent write with same event id does not append event" subsequentWriteWithSameEventIdDoesNotAppendSecondEvent,
      testCase "Unit - Subsequent write with same event id accepted if expected version is correct" subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect,
      testCase "Unit - Written Events Appear In Read Stream - explicit expected version" $ writtenEventsAppearInReadStream writeEventsWithExplicitExpectedVersions,
      testCase "Unit - Written Events Appear In Read Stream - explicit expected version" $ writtenEventsAppearInReadStream writeEventsWithNoExpectedVersions,
      testCase "Unit - Cannot write event if previous one does not exist" cannotWriteEventsOutOfOrder,
      testCase "Unit - Can write first event" canWriteFirstEvent,
      testCase "Unit - Check Iops usage" whenIndexing1000ItemsIopsIsMinimal,
      testCase "Unit - Error thrown if trying to write an event in a multiple gap" errorThrownIfTryingToWriteAnEventInAMultipleGap,
      testCase "Unit - EventNumbers are calculated when there are multiple events" eventNumbersCorrectForMultipleEvents,
      testGroup "Stream Link Tests" streamLinkTests
  ]
