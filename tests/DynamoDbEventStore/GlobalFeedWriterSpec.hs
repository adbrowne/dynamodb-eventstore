{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module DynamoDbEventStore.GlobalFeedWriterSpec (tests) where

import           BasicPrelude
import           Control.Lens
import           Control.Monad.Except
import           Control.Monad.Loops
import           Control.Monad.Random
import           Control.Monad.State
import           DodgerBlue.Testing
import qualified Data.Aeson                              as Aeson
import qualified Data.ByteString.Lazy                    as BL
import           Data.Either.Combinators
import           Data.Foldable                           hiding (concat)
import           Data.List.NonEmpty                      (NonEmpty (..))
import qualified Data.List.NonEmpty                      as NonEmpty
import           Data.Map.Strict                         ((!))
import qualified Data.Map.Strict                         as Map
import           Data.Maybe                              (fromJust)
import qualified Data.Sequence                           as Seq
import qualified Data.Set                                as Set
import qualified Data.Text                               as T
import qualified Data.Text.Encoding                      as T
import qualified Data.Text.Lazy.Encoding                 as TL
import           Data.Time.Format
import qualified Data.UUID                               as UUID
import           GHC.Natural
import qualified Pipes.Prelude                           as P
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck                   (testProperty, (===))
import qualified Test.Tasty.QuickCheck                   as QC

import           DynamoDbEventStore.DynamoCmdInterpreter
import           DynamoDbEventStore.Types
import qualified DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreActions (PostEventRequest(..),EventEntry(..),EventWriteResult(..),EventStartPosition(..),FeedDirection(..),StreamResult(..),GlobalStartPosition(..),GlobalFeedPosition(..),GlobalStreamResult(..),StreamOffset,ReadStreamRequest(..),ReadAllRequest(..),EventType(..),EventTime(..),unEventTime,ReadEventRequest(..),recordedEventProducerBackward)
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.GlobalFeedWriter     (EventStoreActionError (..))
import qualified DynamoDbEventStore.GlobalFeedWriter     as GlobalFeedWriter

postEventRequestProgram :: MonadEsDsl m => PostEventRequest -> m (Either EventStoreActionError EventWriteResult)
postEventRequestProgram = runExceptT . DynamoDbEventStore.EventStoreActions.postEventRequestProgram
getReadAllRequestProgram :: MonadEsDsl m => ReadAllRequest -> m (Either EventStoreActionError GlobalStreamResult)
getReadAllRequestProgram = runExceptT . DynamoDbEventStore.EventStoreActions.getReadAllRequestProgram
getReadEventRequestProgram :: MonadEsDsl m => ReadEventRequest -> m (Either EventStoreActionError (Maybe RecordedEvent))
getReadEventRequestProgram = runExceptT . DynamoDbEventStore.EventStoreActions.getReadEventRequestProgram
getReadStreamRequestProgram :: MonadEsDsl m => ReadStreamRequest -> m (Either EventStoreActionError (Maybe StreamResult))
getReadStreamRequestProgram = runExceptT . DynamoDbEventStore.EventStoreActions.getReadStreamRequestProgram
globalFeedWriterProgram :: MonadEsDslWithFork m => m (Either EventStoreActionError ())
globalFeedWriterProgram = runExceptT (evalStateT GlobalFeedWriter.main GlobalFeedWriter.emptyGlobalFeedWriterState)

type UploadItem = (Text,Int64,NonEmpty EventEntry)
newtype UploadList = UploadList [UploadItem] deriving (Show)

sampleTime :: EventTime
sampleTime = EventTime $ parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000"

eventIdFromString :: String -> EventId
eventIdFromString = EventId . fromJust . UUID.fromString

sampleEventId :: EventId
sampleEventId = eventIdFromString "c2cc10e1-57d6-4b6f-9899-38d972112d8c"

buildSampleEvent :: Text -> EventEntry
buildSampleEvent eventType = EventEntry (TL.encodeUtf8 "My Content") (EventType eventType) sampleEventId sampleTime False

sampleEventEntry :: EventEntry
sampleEventEntry = buildSampleEvent "MyEvent"

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen [a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

uniqueList :: Ord a => [a] -> [a]
uniqueList = Set.toList . Set.fromList

data NewUploadList = NewUploadList { uploadListEvents :: [NonEmpty (NonEmpty EventEntry)], uploadListGenSeed :: Int }
  deriving (Show)

instance QC.Arbitrary NewUploadList where
  arbitrary =
    NewUploadList <$> QC.resize 10 QC.arbitrary <*> QC.arbitrary
  shrink NewUploadList{..} =
    (\xs -> NewUploadList xs uploadListGenSeed) <$> QC.shrink uploadListEvents

instance QC.Arbitrary a => QC.Arbitrary (NonEmpty a) where
  arbitrary = do
    headEntry <- QC.arbitrary
    tailEntries <- QC.arbitrary
    return $ headEntry :| tailEntries
  shrink (x :| xs) =
    let
      shrunkHeads = (:| xs) <$> QC.shrink x
      shrunkTails = (x :|) <$> QC.shrink xs
    in shrunkTails ++ shrunkHeads -- do this in reverse as shrinking the tails is a bigger change

getRandomMapEntry :: MonadRandom m => Map a b -> m (a, b)
getRandomMapEntry m = do
  let myEntries = Map.assocs m
  entryIndex <- getRandomR (0, length myEntries - 1)
  return $ myEntries !! entryIndex

type EventInsertSet = NonEmpty EventEntry

buildUploadListItems :: NewUploadList -> [UploadItem]
buildUploadListItems NewUploadList{..} =
  let
    (streamsWithEvents :: Map Text [(Int64, NonEmpty EventEntry)]) = Map.fromList $ zip streamIds (numberTestStreamEvents . NonEmpty.toList <$> uploadListEvents)
    selectRandomItems :: MonadRandom m => Map Text [(Int64, EventInsertSet)] -> m [UploadItem]
    selectRandomItems fullMap = reverse <$> go [] fullMap
      where
        go :: MonadRandom m => [UploadItem] -> Map Text [(Int64, EventInsertSet)] -> m [UploadItem]
        go a m | Map.null m = return a
        go a m = do
          (streamId, (eventNumber, entries):xs) <- getRandomMapEntry m
          let a' = (streamId, eventNumber, entries):a
          let m' =
                if null xs then
                  Map.delete streamId m
                else
                  Map.insert streamId xs m
          go a' m'
  in runIdentity . (evalRandT (selectRandomItems streamsWithEvents)) $ (mkStdGen uploadListGenSeed)
  where
    streamIds = (\x -> T.pack $ "testStream-" <> [x]) <$> ['a'..'z']
    numberTestStreamEvents :: [NonEmpty EventEntry] -> [(Int64, EventInsertSet)]
    numberTestStreamEvents xs = reverse $ evalState (foldM acc [] xs) (-1)
    acc :: [(Int64, EventInsertSet)] -> EventInsertSet -> State Int64 [(Int64, EventInsertSet)]
    acc xs x = do
      (eventNumber :: Int64) <- get
      put (eventNumber + (fromIntegral . length) x)
      return $ (eventNumber, x):xs

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

writeEvent :: (Text, Int64, NonEmpty EventEntry) -> DynamoCmdM Queue (Either EventStoreActionError EventWriteResult)
writeEvent (stream, eventNumber, eventEntries) = do
  log' Debug "Writing Item"
  postEventRequestProgram (PostEventRequest stream (Just eventNumber) eventEntries)

publisher :: [(Text, Int64, NonEmpty EventEntry)] -> DynamoCmdM Queue (Either EventStoreActionError ())
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

globalStreamResultToMap :: GlobalStreamResult -> Map.Map Text (Seq.Seq Int64)
globalStreamResultToMap GlobalStreamResult{..} =
  foldl' acc Map.empty globalStreamResultEvents
  where
    acc :: Map.Map Text (Seq.Seq Int64) -> RecordedEvent -> Map.Map Text (Seq.Seq Int64)
    acc s RecordedEvent {..} =
      let newValue = maybe (Seq.singleton recordedEventNumber) (Seq.|> recordedEventNumber) $ Map.lookup recordedEventStreamId s
      in Map.insert recordedEventStreamId newValue s

prop_EventShouldAppearInGlobalFeedInStreamOrder :: NewUploadList -> QC.Property
prop_EventShouldAppearInGlobalFeedInStreamOrder newUploadList =
  let
    uploadList = buildUploadListItems newUploadList
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100))
      , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
      , ("GlobalFeedWriter2", (globalFeedWriterProgram, 100))
      ]
  in QC.forAll (runPrograms programs) (check uploadList)
     where
       check uploadList (_, testRunState) = QC.forAll (runReadAllProgram testRunState) (\feedItems -> (globalStreamResultToMap <$> feedItems) === (Right $ globalFeedFromUploadList uploadList))
       runReadAllProgram = runProgramGenerator "readAllRequestProgram" (getReadAllRequestProgram ReadAllRequest { readAllRequestStartPosition = Nothing, readAllRequestMaxItems = 10000, readAllRequestDirection = FeedDirectionForward })

prop_SingleEventIsIndexedCorrectly :: QC.Property
prop_SingleEventIsIndexedCorrectly =
  let
    uploadList = [("MyStream",-1, sampleEventEntry :| [])]
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100))
      , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
      , ("GlobalFeedWriter2", (globalFeedWriterProgram, 100))
      ]
  in QC.forAll (runPrograms programs) (check uploadList)
     where
       check uploadList (_, testRunState) = QC.forAll (runReadAllProgram testRunState) (\feedItems -> (globalStreamResultToMap <$> feedItems) === (Right $ globalFeedFromUploadList uploadList))
       runReadAllProgram = runProgramGenerator "readAllRequestProgram" (getReadAllRequestProgram ReadAllRequest { readAllRequestStartPosition = Nothing, readAllRequestMaxItems = 2000, readAllRequestDirection = FeedDirectionForward })

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
    readStreamEvents streamId startEvent maxItems = fmap3 recordedEventNumber $ fmap2 streamResultEvents $ evalProgram "ReadStream" (getReadStreamRequestProgram (ReadStreamRequest streamId startEvent maxItems FeedDirectionForward)) writeState
    expectedEvents streamId startEvent maxItems = take (fromIntegral maxItems) $ drop (fromMaybe 0 startEvent) $ toList $ expectedStreamEvents ! streamId
    check (streamId, startEvent, maxItems) = readStreamEvents (StreamId streamId) ((fromIntegral . unpositive) <$> startEvent) maxItems === Right (Just (expectedEvents streamId (unpositive <$> startEvent) maxItems))
  in QC.forAll ((,,) <$> (QC.elements . Map.keys) expectedStreamEvents <*> QC.arbitrary <*> QC.arbitrary) check

prop_CanReadAnySectionOfAStreamBackward :: UploadList -> QC.Property
prop_CanReadAnySectionOfAStreamBackward (UploadList uploadList) =
  let
    writeState = execProgram "publisher" (publisher uploadList) emptyTestState
    expectedStreamEvents = globalFeedFromUploadList uploadList
    readStreamEvents streamId startEvent maxItems = fmap3 recordedEventNumber $ fmap2 streamResultEvents $ evalProgram "ReadStream" (getReadStreamRequestProgram (ReadStreamRequest streamId startEvent maxItems FeedDirectionBackward)) writeState
    expectedEvents streamId Nothing maxItems = take (fromIntegral maxItems) $ reverse $ toList $ expectedStreamEvents ! streamId
    expectedEvents streamId (Just startEvent) maxItems = takeWhile (> startEvent - fromIntegral maxItems) $ dropWhile (> startEvent) $ reverse $ toList $ expectedStreamEvents ! streamId
    check (streamId, startEvent, maxItems) = QC.counterexample (T.unpack $ show $ writeState) $ readStreamEvents (StreamId streamId) ((fromIntegral . unpositive) <$> startEvent) maxItems === Right (Just (expectedEvents streamId (fromIntegral . unpositive <$> startEvent) maxItems))
  in QC.forAll ((,,) <$> (QC.elements . Map.keys) expectedStreamEvents <*> QC.arbitrary <*> QC.arbitrary) check

expectedEventsFromUploadList :: UploadList -> [RecordedEvent]
expectedEventsFromUploadList (UploadList uploadItems) = do
  (streamId, firstEventNumber, eventEntries) <- uploadItems
  (eventNumber, EventEntry eventData (EventType eventType) eventId (EventTime eventTime) isJson) <- zip [firstEventNumber+1..] (NonEmpty.toList eventEntries)
  return RecordedEvent {
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
    lookupBodies testRunState = fmap (\RecordedEvent{..} -> lookupBody testRunState recordedEventStreamId recordedEventNumber) expectedEvents
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
       check (writeResults, _testState) = foldl' sumIfSuccess 0 writeResults === 1
       sumIfSuccess :: Int -> Either e EventWriteResult -> Int
       sumIfSuccess s (Right WriteSuccess) = s + 1
       sumIfSuccess s _            = s

pageThroughGlobalFeed :: Natural -> DynamoCmdM Queue (Either EventStoreActionError [RecordedEvent])
pageThroughGlobalFeed pageSize =
  go [] Nothing
  where
    go acc nextPosition = do
      result <- getReadAllRequestProgram ReadAllRequest {
        readAllRequestDirection = FeedDirectionBackward,
        readAllRequestStartPosition = nextPosition,
        readAllRequestMaxItems = pageSize }
      either (return . Left) (processResult acc) result
    processResult :: [RecordedEvent] -> GlobalStreamResult -> DynamoCmdM Queue (Either EventStoreActionError [RecordedEvent])
    processResult acc GlobalStreamResult{globalStreamResultNext = Nothing} = (return . Right) acc
    processResult acc GlobalStreamResult{globalStreamResultNext = Just(_,nextPosition,_),..} =
      go (globalStreamResultEvents ++ acc) (convertGlobalStartPosition nextPosition)
    convertGlobalStartPosition GlobalStartHead = Nothing
    convertGlobalStartPosition (GlobalStartPosition x) = Just x


getStreamRecordedEvents :: Text -> ExceptT EventStoreActionError (DynamoCmdM Queue) [RecordedEvent]
getStreamRecordedEvents streamId = do
   recordedEvents <- concat <$> unfoldrM getEventSet Nothing
   return $ reverse recordedEvents
   where
    getEventSet :: Maybe Int64 -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Maybe ([RecordedEvent], Maybe Int64))
    getEventSet startEvent =
      if ((< 0) <$> startEvent) == Just True then
        return Nothing
      else do
        streamResult <- lift (getReadStreamRequestProgram (ReadStreamRequest (StreamId streamId) startEvent 10 FeedDirectionBackward)) >>= eitherToError
        let result = streamResultEvents <$> streamResult
        if result == Just [] then
          return Nothing
        else
          return $ (\recordedEvents -> (recordedEvents, (Just . (\x -> x - 1) . recordedEventNumber . last) recordedEvents)) <$> result

readEachStream :: [UploadItem] -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Map.Map Text (Seq.Seq Int64))
readEachStream uploadItems =
  foldM readStream Map.empty streams
  where
    readStream :: Map.Map Text (Seq.Seq Int64) -> Text -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Map.Map Text (Seq.Seq Int64))
    readStream m streamId = do
      eventIds <- getEventIds streamId
      return $ Map.insert streamId eventIds m
    getEventIds :: Text -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Seq.Seq Int64)
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
      ("GlobalFeedWriter1", (globalFeedWriterProgram, 100)),
      ("GlobalFeedWriter2", (globalFeedWriterProgram, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testRunState) = runReadEachStream testRunState === (Right $ globalFeedFromUploadList uploadList)
       runReadEachStream = evalProgram "readEachStream" (runExceptT (readEachStream uploadList))

prop_ScanUnpagedShouldBeEmpty :: UploadList -> QC.Property
prop_ScanUnpagedShouldBeEmpty (UploadList uploadList) =
  let
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (globalFeedWriterProgram, 100)),
      ("GlobalFeedWriter2", (globalFeedWriterProgram, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testRunState) = scanUnpaged testRunState === []
       scanUnpaged = evalProgram "scanUnpaged" scanNeedsPaging'

type EventWriter = StreamId -> [(Text, EventId, LByteString)] -> DynamoCmdM Queue ()

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

writeThenRead :: StreamId -> [(Text, EventId, LByteString)] -> EventWriter -> ExceptT EventStoreActionError (DynamoCmdM Queue) [RecordedEvent]
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
      , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
      ]
  in QC.forAll (runPrograms programs) check
     where
       -- global feed writer runs forever unless there is an error so we don't
       -- expect a result
       check (results, _) = Map.lookup "GlobalFeedWriter1" results === Nothing

cannotWriteEventsOutOfOrder :: Assertion
cannotWriteEventsOutOfOrder =
  let
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just 1, perEvents = sampleEventEntry :| [] }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return an error" (Right WrongExpectedVersion) result

canWriteFirstEvent :: Assertion
canWriteFirstEvent =
  let
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Just (-1), perEvents = sampleEventEntry :| [] }
    result = evalProgram "writeEvent" (postEventRequestProgram postEventRequest) emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

secondSampleEventEntry :: EventEntry
secondSampleEventEntry = sampleEventEntry { eventEntryType = EventType "My Event2", eventEntryData = TL.encodeUtf8 "My Content2"}
eventNumbersCorrectForMultipleEvents :: Assertion
eventNumbersCorrectForMultipleEvents =
  let
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = sampleEventEntry :| [secondSampleEventEntry] }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just 1, perEvents = sampleEventEntry :| [] }
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
    postProgram (eventId, eventNumber) = postEventRequestProgram PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEvents = sampleEventEntry { eventEntryType = (EventType . show) eventNumber, eventEntryEventId = eventId } :| [] }
    requests = take itemCount $ postProgram <$> zip sampleEventIds [(0::Int)..]
    writeState = execProgram "writeEvents" (forM_ requests id) emptyTestState
    feedEntries = (\x -> FeedEntry { feedEntryCount = 1, feedEntryNumber = fromIntegral x, feedEntryStream = StreamId streamId}) <$> [0..itemCount-1]
    pages = zip  [0..] (Seq.fromList <$> groupByFibs feedEntries)
    writePage' (pageNumber, pageEntries) = GlobalFeedWriter.writePage pageNumber pageEntries 0
    writePagesProgram = runExceptT $ evalStateT (forM_ pages writePage') GlobalFeedWriter.emptyGlobalFeedWriterState
    globalFeedCreatedState = execProgram "writeGlobalFeed" writePagesProgram writeState
  in globalFeedCreatedState

getSampleItems :: Maybe Int64 -> Natural -> FeedDirection -> Either EventStoreActionError (Maybe StreamResult)
getSampleItems startEvent maxItems direction =
  evalProgram "ReadStream" (getReadStreamRequestProgram (ReadStreamRequest (StreamId "MyStream") startEvent maxItems direction)) (testStateItems 29)

getSampleGlobalItems :: Maybe GlobalFeedPosition -> Natural -> FeedDirection -> Either EventStoreActionError GlobalStreamResult
getSampleGlobalItems startPosition maxItems direction =
  let
    readAllRequest = ReadAllRequest startPosition maxItems direction
    programState = testStateItems 29
  in evalProgram "ReadAllStream" (getReadAllRequestProgram readAllRequest) programState

fibs :: [Int]
fibs =
  let acc (a,b) = Just (a+b,(b,a+b))
  in 1:1:unfoldr acc (1,1)

groupByFibs :: [a] -> [[a]]
groupByFibs as =
  let
    acc (_, []) = Nothing
    acc ([], _) = error "ran out of fibs that should not happen"
    acc (x:xs, ys) = Just (take x ys, (xs, drop x ys))
  in unfoldr acc (fibs,as)

readStreamProgram :: Text -> Natural -> FeedDirection -> DynamoCmdM Queue [Int64]
readStreamProgram streamId pageSize direction =
  let
    streamResultLink =
      case direction of FeedDirectionBackward -> streamResultNext
                        FeedDirectionForward  -> streamResultPrevious
    request startEventNumber = ReadStreamRequest {
      rsrStreamId = StreamId streamId
    , rsrMaxItems = pageSize
    , rsrStartEventNumber = startEventNumber
    , rsrDirection = direction }
    positionToRequest EventStartHead = request Nothing
    positionToRequest (EventStartPosition p) = request $ Just p
    getResultEventNumbers :: StreamResult -> ([Int64], Maybe StreamOffset)
    getResultEventNumbers streamResult@StreamResult{..} =
      (recordedEventNumber <$> streamResultEvents, streamResultLink streamResult)
    start :: Maybe StreamOffset
    start = Just $ (FeedDirectionBackward, EventStartHead, pageSize)
    acc :: Maybe StreamOffset -> DynamoCmdM Queue (Maybe ([Int64], Maybe StreamOffset))
    acc Nothing = return Nothing
    acc (Just (_, position, _)) =
      either (const Nothing) (fmap getResultEventNumbers) <$> getReadStreamRequestProgram (positionToRequest position)
  in
    join <$> unfoldrM acc start

prop_all_items_are_in_stream_when_paged_through :: QC.Positive Natural -> QC.Positive Int -> FeedDirection -> QC.Property
prop_all_items_are_in_stream_when_paged_through (QC.Positive pageSize) (QC.Positive streamLength) direction =
  let
    startState = testStateItems streamLength
    program = readStreamProgram "MyStream" pageSize direction
    programResult =
      evalProgram "readStream" program startState
    maxEventNumber = fromIntegral $ streamLength - 1
    expectedResult =
      case direction of FeedDirectionForward  -> [0..maxEventNumber]
                        FeedDirectionBackward -> [maxEventNumber, maxEventNumber - 1..0]

  in
    programResult === expectedResult

{-
globalStreamPages:
0: 0 (1)
1: 1 (1)
2: 2,3 (2)
3: 4,5,6 (3)
4: 7,8,9,10,11 (5)
5: 12,13,14,15,16,17,18,19 (8)
6: 20,21,22,23,24,25,26,27,28,29,30,31,32 (13)
7: 33..53 (21)
8: 54..87 (34)
9: 88,89,90,91,92,93,94,95,96,97,98,99,100.. (55)
-}

globalStreamPagingTests :: [TestTree]
globalStreamPagingTests =
  let
    getEventTypes start maxItems direction = fmap2 recordedEventType $ globalStreamResultEvents <$> getSampleGlobalItems start maxItems direction
    resultAssert testName start maxItems direction expectedBodies =
      testCase testName $ assertEqual "Should return events" (Right expectedBodies) (getEventTypes start maxItems direction)
  in [
      resultAssert "Start of feed forward - start = Nothing" Nothing 1 FeedDirectionForward ["0"]
    , resultAssert "0 0 of feed forward" (Just $ GlobalFeedPosition 0 0) 1 FeedDirectionForward ["1"]
    , resultAssert "Middle of the feed forward" (Just $ GlobalFeedPosition 1 0) 3 FeedDirectionForward ["2","3","4"]
    , resultAssert "End of the feed forward" (Just $ GlobalFeedPosition 6 7) 3 FeedDirectionForward ["28"]
    , testCase "Past End of the feed forward" $ assertEqual "Should return error" (Left (EventStoreActionErrorInvalidGlobalFeedPosition (GlobalFeedPosition {globalFeedPositionPage = 6, globalFeedPositionOffset = 9}))) (getEventTypes (Just $ GlobalFeedPosition 6 9) 3 FeedDirectionBackward)
    , testCase "Before the end of feed backward" $ assertEqual "Should return error" (Left (EventStoreActionErrorInvalidGlobalFeedPosition (GlobalFeedPosition {globalFeedPositionPage = 9, globalFeedPositionOffset = 0}))) (getEventTypes (Just $ GlobalFeedPosition 9 0) 3 FeedDirectionBackward)
    , resultAssert "End of feed backward - start = Nothing" Nothing 3 FeedDirectionBackward ["28","27","26"]
    , resultAssert "End of the feed backward" (Just $ GlobalFeedPosition 6 8) 3 FeedDirectionBackward ["28","27","26"]
    , resultAssert "Middle of the feed backward" (Just $ GlobalFeedPosition 5 7) 3 FeedDirectionBackward ["19","18","17"]
    , resultAssert "End of feed backward" (Just $ GlobalFeedPosition 0 0) 1 FeedDirectionBackward ["0"]
  ]

globalStreamLinkTests :: [TestTree]
globalStreamLinkTests =
  let
    toFeedPosition page offset = Just GlobalFeedPosition {
        globalFeedPositionPage = page
      , globalFeedPositionOffset = offset }
    endOfFeedBackward = ("End of feed backward", getSampleGlobalItems Nothing 20 FeedDirectionBackward)
    middleOfFeedBackward = ("Middle of feed backward", getSampleGlobalItems (toFeedPosition 6 5) 20 FeedDirectionBackward)
    startOfFeedBackward = ("Start of feed backward", getSampleGlobalItems (toFeedPosition 1 0) 20 FeedDirectionBackward)
    startOfFeedForward = ("Start of feed forward", getSampleGlobalItems Nothing 20 FeedDirectionForward)
    middleOfFeedForward = ("Middle of feed forward", getSampleGlobalItems (toFeedPosition 2 1) 20 FeedDirectionForward)
    endOfFeedForward = ("End of feed forward", getSampleGlobalItems (toFeedPosition 6 8) 20 FeedDirectionForward)
    streamResultLast' = ("last", globalStreamResultLast)
    streamResultFirst' = ("first", globalStreamResultFirst)
    streamResultNext' = ("next", globalStreamResultNext)
    streamResultPrevious' = ("previous", globalStreamResultPrevious)
    toStartPosition page offset = GlobalStartPosition $ GlobalFeedPosition page offset
    linkAssert (feedResultName, feedResult) (linkName, streamLink) expectedResult =
      testCase
        ("Unit - " <> feedResultName <> " - " <> linkName <> " link") $
        assertEqual
          ("Should have " <> linkName <> " link")
          (Right expectedResult)
          (fmap streamLink feedResult)
  in [
      linkAssert endOfFeedBackward streamResultFirst' (Just  (FeedDirectionBackward, GlobalStartHead, 20))
    , linkAssert endOfFeedBackward streamResultLast' (Just (FeedDirectionForward, GlobalStartHead, 20))
    , linkAssert endOfFeedBackward streamResultNext' (Just (FeedDirectionBackward, toStartPosition 4 1, 20))
    , linkAssert endOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, toStartPosition 6 8, 20))
    , linkAssert middleOfFeedBackward streamResultFirst' (Just (FeedDirectionBackward, GlobalStartHead, 20))
    , linkAssert middleOfFeedBackward streamResultLast' (Just (FeedDirectionForward, GlobalStartHead, 20))
    , linkAssert middleOfFeedBackward streamResultNext' (Just (FeedDirectionBackward, toStartPosition 3 1, 20))
    , linkAssert middleOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, toStartPosition 6 5, 20))
    , linkAssert startOfFeedBackward streamResultFirst' (Just (FeedDirectionBackward, GlobalStartHead, 20))
    , linkAssert startOfFeedBackward streamResultLast' Nothing
    , linkAssert startOfFeedBackward streamResultNext' Nothing
    , linkAssert startOfFeedBackward streamResultPrevious' (Just (FeedDirectionForward, toStartPosition 1 0, 20))
    , linkAssert startOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, GlobalStartHead, 20))
    , linkAssert startOfFeedForward streamResultLast' Nothing
    , linkAssert startOfFeedForward streamResultNext' Nothing
    , linkAssert startOfFeedForward streamResultPrevious' (Just (FeedDirectionForward, toStartPosition 5 7, 20))
    , linkAssert middleOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, GlobalStartHead, 20))
    , linkAssert middleOfFeedForward streamResultLast' (Just (FeedDirectionForward, GlobalStartHead, 20))
    , linkAssert middleOfFeedForward streamResultNext' (Just (FeedDirectionBackward, toStartPosition 2 1, 20))
    , linkAssert middleOfFeedForward streamResultPrevious' (Just (FeedDirectionForward, toStartPosition 6 3, 20))
    , linkAssert endOfFeedForward streamResultFirst' (Just (FeedDirectionBackward, GlobalStartHead, 20))
    , linkAssert endOfFeedForward streamResultLast' (Just (FeedDirectionForward, GlobalStartHead, 20))
    , linkAssert endOfFeedForward streamResultNext' (Just (FeedDirectionBackward, toStartPosition 6 8, 20))
    , linkAssert endOfFeedForward streamResultPrevious' Nothing
  ]

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
    afterIndexState = execProgramUntilIdle "indexer" globalFeedWriterProgram (testStateItems 1000)
    afterReadState = execProgramUntilIdle "globalFeedReader" (pageThroughGlobalFeed 10) afterIndexState
    expectedWriteState = Map.fromList [
      ((UnpagedRead,IopsScanUnpaged,"indexer"),1000)
     ,((TableRead,IopsGetItem,"indexer"),1019)
     ,((TableRead,IopsGetItem,"globalFeedReader"),232)
     ,((TableRead,IopsQuery,"indexer"),1984)
     ,((TableRead,IopsQuery,"globalFeedReader"),2184)
     ,((TableRead,IopsQuery,"writeEvents"),999)
     ,((TableWrite,IopsWrite,"indexer"),1003)
     ,((TableWrite,IopsWrite,"writeGlobalFeed"),15)
     ,((TableWrite,IopsWrite,"writeEvents"),1000)]
  in assertEqual "Should be small iops" expectedWriteState (view iopCounts afterReadState)

errorThrownIfTryingToWriteAnEventInAMultipleGap :: Assertion
errorThrownIfTryingToWriteAnEventInAMultipleGap =
  let
    streamId = "MyStream"
    multiPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = sampleEventEntry :| [secondSampleEventEntry] }
    subsequentPostEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Just (-1), perEvents = sampleEventEntry :| [] }
    result = evalProgram "writeEvents" (postEventRequestProgram multiPostEventRequest >> postEventRequestProgram subsequentPostEventRequest) emptyTestState
  in assertEqual "Should return failure" (Right EventExists) result

postTwoEventWithTheSameEventId :: DynamoCmdM Queue (Either EventStoreActionError EventWriteResult)
postTwoEventWithTheSameEventId =
  let
    postEventRequest = PostEventRequest { perStreamId = "MyStream", perExpectedVersion = Nothing, perEvents = sampleEventEntry :| [] }
  in postEventRequestProgram postEventRequest >> postEventRequestProgram postEventRequest

subsequentWriteWithSameEventIdReturnsSuccess :: Assertion
subsequentWriteWithSameEventIdReturnsSuccess =
  let
    result = evalProgram "test" postTwoEventWithTheSameEventId emptyTestState
  in assertEqual "Should return success" (Right WriteSuccess) result

subsequentWriteWithSameEventIdDoesNotAppendSecondEvent :: Assertion
subsequentWriteWithSameEventIdDoesNotAppendSecondEvent =
  let
    program = postTwoEventWithTheSameEventId >> runExceptT (getStreamRecordedEvents "MyStream")
    result = evalProgram "test" program emptyTestState
  in assertEqual "Should return a single event" (Right 1) (length <$> result)

subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents :: Assertion
subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents =
  let
    streamId = "MyStream"
    (eventId1:eventId2:_) = sampleEventIds
    postEvents events =  postEventRequestProgram PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEvents = events }
    postDoubleEvents = postEvents $ sampleEventEntry { eventEntryEventId = eventId1 } :| [ sampleEventEntry { eventEntryEventId = eventId2 } ]
    postSubsequentEvent = postEvents $ sampleEventEntry { eventEntryEventId = eventId1 } :| []
    program =
      postDoubleEvents >>
      postSubsequentEvent >>
      runExceptT (getStreamRecordedEvents "MyStream")
    result = evalProgram "test" program emptyTestState
  in assertEqual "Should return the first two events" (Right 2) (length <$> result)

subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect :: Assertion
subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect =
  let
    streamId = "MyStream"
    postEventRequest = PostEventRequest { perStreamId = streamId, perExpectedVersion = Nothing, perEvents = sampleEventEntry :| [] }
    secondPost = postEventRequest { perExpectedVersion = Just 0 }
    program = postEventRequestProgram postEventRequest >> postEventRequestProgram secondPost >> runExceptT (getStreamRecordedEvents streamId)
    result = evalProgram "test" program emptyTestState
  in assertEqual "Should return two events" (Right 2) (length <$> result)

tests :: [TestTree]
tests = [
      testProperty "Can round trip FeedEntry via JSON" (\(a :: FeedEntry) -> (Aeson.decode . Aeson.encode) a === Just a),
      testProperty "Single event is indexed correctly" prop_SingleEventIsIndexedCorrectly,
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder,
      testProperty "Each event appears in it's correct stream" prop_EventsShouldAppearInTheirSteamsInOrder,
      testProperty "No Write Request can cause a fatal error in global feed writer" prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter,
      testProperty "Conflicting writes will not succeed" prop_ConflictingWritesWillNotSucceed,
      testProperty "All Events can be read individually" prop_AllEventsCanBeReadIndividually,
      testProperty "Scan unpaged should be empty" prop_ScanUnpagedShouldBeEmpty,
      testProperty "Can read any section of a stream forward" prop_CanReadAnySectionOfAStreamForward,
      testProperty "Can read any section of a stream backward" prop_CanReadAnySectionOfAStreamBackward,
      testProperty "All items are in the stream when paged through" prop_all_items_are_in_stream_when_paged_through,
      --testProperty "Moving next then previous returns the initial page in reverse order" todo,
      --testProperty "Moving previous then next returns the initial page in reverse order" todo,
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
      testGroup "Single Stream Link Tests" streamLinkTests,
      testGroup "Global Stream Link Tests" globalStreamLinkTests,
      testGroup "Global Stream Paging Tests" globalStreamPagingTests
  ]
