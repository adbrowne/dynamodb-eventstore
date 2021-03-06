{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -fno-warn-orphans  #-}

module DynamoDbEventStore.GlobalFeedWriterSpec
  (tests)
  where

import DynamoDbEventStore.ProjectPrelude
import Control.Lens
import Control.Monad.Random
import Control.Monad.State
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as BL
import Data.Char (isAlpha)
import qualified Control.Foldl as Foldl
import Data.Foldable hiding (concat)
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NonEmpty
import Data.Map.Strict ((!))
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy.Encoding as TL
import Data.Time.Format
import qualified Data.UUID as UUID
import DodgerBlue.Testing
import qualified Pipes.Prelude as P
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck (testProperty, (===))
import qualified Test.Tasty.QuickCheck as QC
import qualified DynamoDbEventStore.Streams as Streams
import DynamoDbEventStore.Streams (EventWriteResult(..))
import qualified DynamoDbEventStore.Storage.StreamItem as StreamItem
import DynamoDbEventStore.Storage.StreamItem (EventEntry(..),EventTime(..),EventType(..),unEventTime)
import DynamoDbEventStore.DynamoCmdInterpreter
import DynamoDbEventStore.EventStoreCommands
import qualified DynamoDbEventStore.GlobalFeedWriter
       as GlobalFeedWriter
import qualified DynamoDbEventStore.Storage.GlobalStreamItem
       as GlobalFeedItem
import DynamoDbEventStore.Types

data WriteEventData = WriteEventData {
   writeEventDataStreamId        :: StreamId,
   writeEventDataExpectedVersion :: Maybe Int64,
   writeEventDataEvents          :: NonEmpty EventEntry }
  deriving (Show)

instance QC.Arbitrary WriteEventData where
  arbitrary = WriteEventData <$> (StreamId . fromString <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> ((:|) <$> QC.arbitrary <*> QC.arbitrary)

postEventRequestProgram
    :: MonadEsDsl m
    => WriteEventData -> m (Either EventStoreActionError Streams.EventWriteResult)
postEventRequestProgram WriteEventData {..} = 
    runExceptT $
      Streams.writeEvent
        writeEventDataStreamId
        writeEventDataExpectedVersion
        writeEventDataEvents

globalFeedWriterProgram
    :: MonadEsDslWithFork m
    => m (Either EventStoreActionError ())
globalFeedWriterProgram = 
    runExceptT
        (evalStateT
             GlobalFeedWriter.main
             GlobalFeedWriter.emptyGlobalFeedWriterState)

testStreamId :: Text
testStreamId = "MyStream"

type UploadItem = (Text, Int64, NonEmpty EventEntry)

newtype UploadList =
    UploadList [UploadItem]
    deriving ((Show))

sampleTime :: StreamItem.EventTime
sampleTime = 
    EventTime $
    parseTimeOrError
        True
        defaultTimeLocale
        rfc822DateFormat
        "Sun, 08 May 2016 12:49:41 +0000"

eventIdFromString :: String -> EventId
eventIdFromString = EventId . fromJust . UUID.fromString

sampleEventId :: EventId
sampleEventId = eventIdFromString "c2cc10e1-57d6-4b6f-9899-38d972112d8c"

buildSampleEvent :: Text -> EventEntry
buildSampleEvent eventType = 
    EventEntry
        (TL.encodeUtf8 "My Content")
        (EventType eventType)
        sampleEventId
        sampleTime
        False

sampleEventEntry :: EventEntry
sampleEventEntry = buildSampleEvent "MyEvent"

-- Generateds a list of length between 1 and maxLength
cappedList
    :: QC.Arbitrary a
    => Int -> QC.Gen [a]
cappedList maxLength = 
    QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

uniqueList
    :: Ord a
    => [a] -> [a]
uniqueList = Set.toList . Set.fromList

data NewUploadList = NewUploadList
    { uploadListEvents :: [NonEmpty (NonEmpty EventEntry)]
    , uploadListGenSeed :: Int
    } deriving ((Show))

instance QC.Arbitrary NewUploadList where
    arbitrary = NewUploadList <$> QC.resize 1 QC.arbitrary <*> QC.arbitrary -- todo increase the size
    shrink NewUploadList{..} = 
        (\xs -> 
              NewUploadList xs uploadListGenSeed) <$>
        QC.shrink uploadListEvents

getRandomMapEntry
    :: MonadRandom m
    => Map a b -> m (a, b)
getRandomMapEntry m = do
    let myEntries = Map.assocs m
    entryIndex <- getRandomR (0, length myEntries - 1)
    return $ myEntries !! entryIndex

type EventInsertSet = NonEmpty EventEntry

buildUploadListItems :: NewUploadList -> [UploadItem]
buildUploadListItems NewUploadList{..} = 
    let (streamsWithEvents :: Map Text [(Int64, NonEmpty EventEntry)]) = 
            Map.fromList $
            zip
                streamIds
                (numberTestStreamEvents . NonEmpty.toList <$> uploadListEvents)
        selectRandomItems
            :: MonadRandom m
            => Map Text [(Int64, EventInsertSet)] -> m [UploadItem]
        selectRandomItems fullMap = reverse <$> go [] fullMap
          where
            go
                :: MonadRandom m
                => [UploadItem]
                -> Map Text [(Int64, EventInsertSet)]
                -> m [UploadItem]
            go a m
              | Map.null m = return a
            go a m = do
                (streamId,(eventNumber,entries):xs) <- getRandomMapEntry m
                let a' = (streamId, eventNumber, entries) : a
                let m' = 
                        if null xs
                            then Map.delete streamId m
                            else Map.insert streamId xs m
                go a' m'
    in runIdentity . (evalRandT (selectRandomItems streamsWithEvents)) $
       (mkStdGen uploadListGenSeed)
  where
    streamIds = 
        (\x -> 
              T.pack $ "testStream-" <> [x]) <$>
        ['a' .. 'z']
    numberTestStreamEvents :: [NonEmpty EventEntry] -> [(Int64, EventInsertSet)]
    numberTestStreamEvents xs = reverse $ evalState (foldM acc [] xs) (-1)
    acc
        :: [(Int64, EventInsertSet)]
        -> EventInsertSet
        -> State Int64 [(Int64, EventInsertSet)]
    acc xs x = do
        (eventNumber :: Int64) <- get
        put (eventNumber + (fromIntegral . length) x)
        return $ (eventNumber, x) : xs

instance QC.Arbitrary UploadList where
    arbitrary = do
        (streams :: [StreamId]) <- uniqueList <$> cappedList 5
        (streamWithEvents :: [(StreamId, [NonEmpty EventEntry])]) <- 
            mapM
                (\s -> 
                      puts >>=
                      (\p -> 
                            return (s, p)))
                streams
        (withTime :: [(StreamId, [NonEmpty EventEntry])]) <- 
            mapM
                (\(s,es) -> 
                      return (s, es))
                streamWithEvents
        return $ UploadList $ numberedPuts withTime
      where
        numberedPuts
            :: [(StreamId, [NonEmpty EventEntry])]
            -> [(Text, Int64, NonEmpty EventEntry)]
        numberedPuts xs = 
            (\(StreamId s,d) -> 
                  banana s d) =<<
            xs
        banana :: Text
               -> [NonEmpty EventEntry]
               -> [(Text, Int64, NonEmpty EventEntry)]
        banana streamId xs = 
            reverse $ evalState (foldM (apple streamId) [] xs) (-1)
        apple
            :: Text
            -> [(Text, Int64, NonEmpty EventEntry)]
            -> NonEmpty EventEntry
            -> State Int64 [(Text, Int64, NonEmpty EventEntry)]
        apple streamId xs x = do
            (eventNumber :: Int64) <- get
            put (eventNumber + (fromIntegral . length) x)
            return $ (streamId, eventNumber, x) : xs
        puts :: QC.Gen [NonEmpty EventEntry]
        puts = do
            (p :: [()]) <- cappedList 100
            mapM
                (\_ -> 
                      (:|) <$> QC.arbitrary <*> cappedList 9)
                p

writeEvent
    :: (Text, Int64, NonEmpty EventEntry)
    -> DynamoCmdM Queue (Either EventStoreActionError Streams.EventWriteResult)
writeEvent (stream,eventNumber,eventEntries) = do
    log' Debug "Writing Item"
    postEventRequestProgram
        (WriteEventData (StreamId stream) (Just eventNumber) eventEntries)

publisher
    :: [(Text, Int64, NonEmpty EventEntry)]
    -> DynamoCmdM Queue (Either EventStoreActionError ())
publisher xs = Right <$> forM_ xs writeEvent

globalFeedFromUploadList :: [UploadItem] -> Map.Map Text (Seq.Seq Int64)
globalFeedFromUploadList = foldl' acc Map.empty
  where
    acc :: Map.Map Text (Seq.Seq Int64)
        -> UploadItem
        -> Map.Map Text (Seq.Seq Int64)
    acc s (stream,expectedVersion,bodies) = 
        let eventVersions = 
                Seq.fromList $ take (length bodies) [expectedVersion + 1 ..]
            newValue = 
                maybe eventVersions (Seq.>< eventVersions) $
                Map.lookup stream s
        in Map.insert stream newValue s

globalStreamToMapFold :: Foldl.Fold (GlobalFeedPosition, EventKey) (Map.Map Text (Seq.Seq Int64))
globalStreamToMapFold = Foldl.Fold acc mempty id
  where
    acc
        :: Map.Map Text (Seq.Seq Int64)
        -> (GlobalFeedPosition, EventKey)
        -> Map.Map Text (Seq.Seq Int64)
    acc s (_, EventKey (StreamId streamId, eventNumber)) = 
        let newValue =
                maybe
                    (Seq.singleton eventNumber)
                    (Seq.|> eventNumber) $
                Map.lookup streamId s
        in Map.insert streamId newValue s

buildEventMap :: MonadEsDsl m => m (Either EventStoreActionError (Map.Map Text (Seq.Seq Int64)))
buildEventMap =
  runExceptT $ Foldl.purely P.fold globalStreamToMapFold (Streams.globalEventKeysProducer QueryDirectionForward Nothing)

prop_EventShouldAppearInGlobalFeedInStreamOrder :: NewUploadList -> QC.Property
prop_EventShouldAppearInGlobalFeedInStreamOrder newUploadList = 
    let uploadList = buildUploadListItems newUploadList
        programs = 
            Map.fromList
                [ ("Publisher", (publisher uploadList, 100))
                , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
                , ("GlobalFeedWriter2", (globalFeedWriterProgram, 100))]
    in (length uploadList < 4) QC.==>
       QC.forAll (runPrograms programs) (check uploadList)
  where
    check uploadList (_,testRunState) = 
        QC.forAll
            (runReadAllProgram testRunState)
            (\eventMap -> 
                  eventMap ===
                  (Right $ globalFeedFromUploadList uploadList))
    runReadAllProgram = 
        runProgramGenerator
            "readAllRequestProgram"
            buildEventMap

prop_SingleEventIsIndexedCorrectly :: QC.Property
prop_SingleEventIsIndexedCorrectly = 
    let uploadList = [("MyStream", -1, sampleEventEntry :| [])]
        programs = 
            Map.fromList
                [ ("Publisher", (publisher uploadList, 100))
                , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
                , ("GlobalFeedWriter2", (globalFeedWriterProgram, 100))]
    in QC.forAll (runPrograms programs) (check uploadList)
  where
    check uploadList (_,testRunState) = 
        QC.forAll
            (runReadAllProgram testRunState)
            (\eventMap -> 
                  eventMap ===
                  (Right $ globalFeedFromUploadList uploadList))
    runReadAllProgram = 
        runProgramGenerator
            "readAllRequestProgram"
            buildEventMap

unpositive :: QC.Positive Int -> Int
unpositive (QC.Positive x) = x

readPartStream
  :: QueryDirection
     -> TestState
     -> StreamId
     -> Maybe Int64
     -> Natural
     -> Either EventStoreActionError [Int64]
readPartStream direction startState streamId startEvent maxItems =
  evalProgram
      "ReadStream"
      (runExceptT $
          P.toListM $
          Streams.streamEventsProducer direction streamId startEvent 10
          >-> P.take (fromIntegral maxItems)
          >-> P.map recordedEventNumber)
      startState

prop_CanReadAnySectionOfAStreamForward :: UploadList -> QC.Property
prop_CanReadAnySectionOfAStreamForward (UploadList uploadList) = 
    let writeState = 
            execProgram "publisher" (publisher uploadList) emptyTestState
        expectedStreamEvents = globalFeedFromUploadList uploadList
        readStreamEvents = readPartStream QueryDirectionForward writeState
        expectedEvents streamId startEvent maxItems = 
            take (fromIntegral maxItems) $
            drop (fromMaybe 0 startEvent) $
            toList $ expectedStreamEvents ! streamId
        check (streamId,startEvent,maxItems) = 
            readStreamEvents
                (StreamId streamId)
                ((fromIntegral . unpositive) <$> startEvent)
                maxItems ===
            Right
                (expectedEvents
                    streamId
                    (unpositive <$> startEvent)
                    maxItems)
    in QC.forAll
           ((,,) <$> (QC.elements . Map.keys) expectedStreamEvents <*>
            QC.arbitrary <*>
            QC.arbitrary)
           check

prop_CanReadAnySectionOfAStreamBackward :: UploadList -> QC.Property
prop_CanReadAnySectionOfAStreamBackward (UploadList uploadList) = 
    let writeState = 
            execProgram "publisher" (publisher uploadList) emptyTestState
        expectedStreamEvents = globalFeedFromUploadList uploadList
        readStreamEvents = readPartStream QueryDirectionBackward writeState
        expectedEvents streamId Nothing maxItems = 
            take (fromIntegral maxItems) $
            reverse $ toList $ expectedStreamEvents ! streamId
        expectedEvents streamId (Just startEvent) maxItems = 
            take (fromIntegral maxItems) $
            dropWhile (> startEvent) $
            reverse $ toList $ expectedStreamEvents ! streamId
        check (streamId,startEvent,maxItems) = 
            QC.counterexample (show writeState) $
            readStreamEvents
                (StreamId streamId)
                ((fromIntegral . unpositive) <$> startEvent)
                maxItems ===
            Right
                (expectedEvents
                          streamId
                          (fromIntegral . unpositive <$> startEvent)
                          maxItems)
    in QC.forAll
           ((,,) <$> (QC.elements . Map.keys) expectedStreamEvents <*>
            QC.arbitrary <*>
            QC.arbitrary)
           check

expectedEventsFromUploadList :: UploadList -> [RecordedEvent]
expectedEventsFromUploadList (UploadList uploadItems) = do
    (streamId,firstEventNumber,eventEntries) <- uploadItems
    (eventNumber,EventEntry eventData (StreamItem.EventType eventType) eventId (StreamItem.EventTime eventTime) isJson) <- 
        zip [firstEventNumber + 1 ..] (NonEmpty.toList eventEntries)
    return
        RecordedEvent
        { recordedEventStreamId = streamId
        , recordedEventNumber = eventNumber
        , recordedEventData = BL.toStrict eventData
        , recordedEventType = eventType
        , recordedEventId = eventId
        , recordedEventCreated = eventTime
        , recordedEventIsJson = isJson
        }

prop_AllEventsCanBeReadIndividually :: NewUploadList -> QC.Property
prop_AllEventsCanBeReadIndividually newUploadList = 
    let uploadItems = buildUploadListItems newUploadList
        programs = Map.fromList [("Publisher", (publisher uploadItems, 100))]
        expectedEvents = expectedEventsFromUploadList (UploadList uploadItems)
        check (_,testRunState) = 
            lookupBodies testRunState === ((Right . Just) <$> expectedEvents)
        lookupBodies testRunState = 
            fmap
                (\RecordedEvent{..} -> 
                      lookupBody
                          testRunState
                          recordedEventStreamId
                          recordedEventNumber)
                expectedEvents
        lookupBody testRunState streamId eventNumber = 
            (id <$>) <$>
            evalProgram
                "LookupEvent"
                (runExceptT $ Streams.readEvent (StreamId streamId) eventNumber)
                testRunState
    in QC.forAll (runPrograms programs) check

prop_ConflictingWritesWillNotSucceed :: QC.Property
prop_ConflictingWritesWillNotSucceed = 
    let programs = 
            Map.fromList
                [ ( "WriteOne"
                  , ( writeEvent
                          ( "MyStream"
                          , -1
                          , sampleEventEntry :| [sampleEventEntry])
                    , 10))
                , ( "WriteTwo"
                  , (writeEvent ("MyStream", -1, sampleEventEntry :| []), 10))]
    in QC.forAll (runPrograms programs) check
  where
    check (writeResults,_testState) = foldl' sumIfSuccess 0 writeResults === 1
    sumIfSuccess :: Int -> Either e EventWriteResult -> Int
    sumIfSuccess s (Right WriteSuccess) = s + 1
    sumIfSuccess s _ = s

pageThroughGlobalFeed :: Natural
                      -> DynamoCmdM Queue (Either EventStoreActionError [RecordedEvent])
pageThroughGlobalFeed _pageSize = runExceptT $ P.toListM $
    Streams.globalEventsProducer QueryDirectionForward Nothing
    >->
    P.map snd

getStreamRecordedEvents :: Text
                        -> ExceptT EventStoreActionError (DynamoCmdM Queue) [RecordedEvent]
getStreamRecordedEvents streamId = do
    P.toListM $ Streams.streamEventsProducer QueryDirectionForward (StreamId streamId) Nothing 10

readEachStream
    :: [UploadItem]
    -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Map.Map Text (Seq.Seq Int64))
readEachStream uploadItems = foldM readStream Map.empty streams
  where
    readStream
        :: Map.Map Text (Seq.Seq Int64)
        -> Text
        -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Map.Map Text (Seq.Seq Int64))
    readStream m streamId = do
        eventIds <- getEventIds streamId
        return $ Map.insert streamId eventIds m
    getEventIds :: Text
                -> ExceptT EventStoreActionError (DynamoCmdM Queue) (Seq.Seq Int64)
    getEventIds streamId = do
        (recordedEvents :: [RecordedEvent]) <- 
            P.toListM $
            Streams.streamEventsProducer QueryDirectionBackward (StreamId streamId) Nothing 10
        return $
            Seq.fromList . reverse $ (recordedEventNumber <$> recordedEvents)
    streams :: [Text]
    streams = 
        (\(stream,_,_) -> 
              stream) <$>
        uploadItems

prop_EventsShouldAppearInTheirSteamsInOrder :: NewUploadList -> QC.Property
prop_EventsShouldAppearInTheirSteamsInOrder newUploadList = 
    let uploadList = buildUploadListItems newUploadList
        expectedGlobalFeed = Right $ globalFeedFromUploadList uploadList
        programs = 
            Map.fromList
                [ ("Publisher", (publisher uploadList, 100))
                , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
                , ("GlobalFeedWriter2", (globalFeedWriterProgram, 100))]
        check (_,testRunState) = 
            runReadEachStream testRunState === expectedGlobalFeed
        runReadEachStream = 
            evalProgram
                "readEachStream"
                (runExceptT (readEachStream uploadList))
    in QC.forAll (runPrograms programs) check

prop_ScanUnpagedShouldBeEmpty :: NewUploadList -> QC.Property
prop_ScanUnpagedShouldBeEmpty newUploadList = 
    let uploadList = buildUploadListItems newUploadList
        programs = 
            Map.fromList
                [ ("Publisher", (publisher uploadList, 100))
                , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))
                , ("GlobalFeedWriter2", (globalFeedWriterProgram, 100))]
    in QC.forAll (runPrograms programs) check
  where
    check (_,testRunState) = scanUnpaged testRunState === []
    scanUnpaged = evalProgram "scanUnpaged" scanNeedsPaging'

type EventWriter = StreamId -> [(Text, EventId, LByteString)] -> DynamoCmdM Queue ()

writeEventsWithExplicitExpectedVersions :: EventWriter
writeEventsWithExplicitExpectedVersions (StreamId streamId) events = 
    evalStateT (forM_ events writeSingleEvent) (-1)
  where
    writeSingleEvent (et,eventId,ed) = do
        eventNumber <- get
        result <- 
            lift $
            postEventRequestProgram
                (WriteEventData
                     (StreamId streamId)
                     (Just eventNumber)
                     (EventEntry ed (EventType et) eventId sampleTime False :|
                      []))
        when (result /= Right WriteSuccess) $ error "Bad write result"
        put (eventNumber + 1)

writeEventsWithNoExpectedVersions :: EventWriter
writeEventsWithNoExpectedVersions (StreamId streamId) events = 
    forM_ events writeSingleEvent
  where
    writeSingleEvent (et,eventId,ed) = do
        result <- 
            postEventRequestProgram
                (WriteEventData
                     (StreamId streamId)
                     Nothing
                     (EventEntry ed (EventType et) eventId sampleTime False :|
                      []))
        when (result /= Right WriteSuccess) $ error "Bad write result"

writeThenRead
    :: StreamId
    -> [(Text, EventId, LByteString)]
    -> EventWriter
    -> ExceptT EventStoreActionError (DynamoCmdM Queue) [RecordedEvent]
writeThenRead (StreamId streamId) events writer = do
    lift $ writer (StreamId streamId) events
    getStreamRecordedEvents streamId

writtenEventsAppearInReadStream :: EventWriter -> Assertion
writtenEventsAppearInReadStream writer = 
    let streamId = StreamId "MyStream"
        eventId1 = eventIdFromString "f3614cb1-5707-4351-8017-2f7471845a61"
        eventId2 = eventIdFromString "9f14fcaf-7c0a-4132-8574-483f0313d7c9"
        eventDatas = 
            [ ("MyEvent", eventId1, TL.encodeUtf8 "My Content")
            , ("MyEvent2", eventId2, TL.encodeUtf8 "My Content2")]
        expectedResult = 
            Right
                [ RecordedEvent
                  { recordedEventStreamId = "MyStream"
                  , recordedEventNumber = 0
                  , recordedEventData = T.encodeUtf8 "My Content"
                  , recordedEventType = "MyEvent"
                  , recordedEventId = eventId1
                  , recordedEventCreated = unEventTime sampleTime
                  , recordedEventIsJson = False
                  }
                , RecordedEvent
                  { recordedEventStreamId = "MyStream"
                  , recordedEventNumber = 1
                  , recordedEventData = T.encodeUtf8 "My Content2"
                  , recordedEventType = "MyEvent2"
                  , recordedEventId = eventId2
                  , recordedEventCreated = unEventTime sampleTime
                  , recordedEventIsJson = False
                  }]
        result = 
            evalProgram
                "writeThenRead"
                (runExceptT $ writeThenRead streamId eventDatas writer)
                emptyTestState
    in assertEqual
           "Returned events should match input events"
           expectedResult
           result

prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter :: [WriteEventData]
                                                          -> QC.Property
prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter events = 
    let programs = 
            Map.fromList
                [ ( "Publisher"
                  , (Right <$> forM_ events postEventRequestProgram, 100))
                , ("GlobalFeedWriter1", (globalFeedWriterProgram, 100))]
    in QC.forAll (runPrograms programs) check
  where
    -- global feed writer runs forever unless there is an error so we don't
    -- expect a result
    check (results,_) = Map.lookup "GlobalFeedWriter1" results === Nothing

cannotWriteEventsOutOfOrder :: Assertion
cannotWriteEventsOutOfOrder = 
    let postEventRequest = 
            WriteEventData
            { writeEventDataStreamId = StreamId "MyStream"
            , writeEventDataExpectedVersion = Just 1
            , writeEventDataEvents = sampleEventEntry :| []
            }
        result = 
            evalProgram
                "writeEvent"
                (postEventRequestProgram postEventRequest)
                emptyTestState
    in assertEqual "Should return an error" (Right WrongExpectedVersion) result

canWriteFirstEvent :: Assertion
canWriteFirstEvent = 
    let postEventRequest = 
            WriteEventData
            { writeEventDataStreamId = StreamId "MyStream"
            , writeEventDataExpectedVersion = Just (-1)
            , writeEventDataEvents = sampleEventEntry :| []
            }
        result = 
            evalProgram
                "writeEvent"
                (postEventRequestProgram postEventRequest)
                emptyTestState
    in assertEqual "Should return success" (Right WriteSuccess) result

secondSampleEventEntry :: EventEntry
secondSampleEventEntry = 
    sampleEventEntry
    { eventEntryType = EventType "My Event2"
    , eventEntryData = TL.encodeUtf8 "My Content2"
    }

eventNumbersCorrectForMultipleEvents :: Assertion
eventNumbersCorrectForMultipleEvents = 
    let streamId = "MyStream"
        multiWriteEventData = 
            WriteEventData
            { writeEventDataStreamId = StreamId streamId
            , writeEventDataExpectedVersion = Just (-1)
            , writeEventDataEvents = sampleEventEntry :| [secondSampleEventEntry]
            }
        subsequentWriteEventData = 
            WriteEventData
            { writeEventDataStreamId = StreamId streamId
            , writeEventDataExpectedVersion = Just 1
            , writeEventDataEvents = sampleEventEntry :| []
            }
        result = 
            evalProgram
                "writeEvent"
                (runExceptT $
                 lift (postEventRequestProgram multiWriteEventData) >>
                 lift (postEventRequestProgram subsequentWriteEventData) >>
                 getStreamRecordedEvents streamId)
                emptyTestState
        eventNumbers = (recordedEventNumber <$>) <$> result
    in assertEqual "Should return success" (Right [0, 1, 2]) eventNumbers

sampleUUIDs :: [UUID.UUID]
sampleUUIDs = 
    let (startUUID :: UUID.UUID) = read "75e52b45-f4d5-445b-8dba-d3dc9b2b34b4"
        (w0,w1,w2,w3) = UUID.toWords startUUID
        createUUID n = UUID.fromWords w0 w1 w2 (n + w3)
    in createUUID <$> [0 ..]

sampleEventIds :: [EventId]
sampleEventIds = EventId <$> sampleUUIDs

testStateItems :: Int -> TestState
testStateItems itemCount =
    let postProgram (eventId,eventNumber) = 
            postEventRequestProgram
                WriteEventData
                { writeEventDataStreamId = StreamId testStreamId
                , writeEventDataExpectedVersion = Nothing
                , writeEventDataEvents = sampleEventEntry
                  { eventEntryType = (EventType . tshow) eventNumber
                  , eventEntryEventId = eventId
                  } :|
                  []
                }
        requests = 
            take itemCount $ postProgram <$> zip sampleEventIds [(0 :: Int) ..]
        writeState = 
            execProgram "writeEvents" (forM_ requests id) emptyTestState
    in writeState

pagedTestStateItems :: Int -> TestState
pagedTestStateItems itemCount = 
    let
        writeState = testStateItems itemCount
        feedEntries = 
            (\x -> 
                  FeedEntry
                  { feedEntryCount = 1
                  , feedEntryNumber = fromIntegral x
                  , feedEntryStream = StreamId testStreamId
                  }) <$>
            [0 .. itemCount - 1]
        pages = zip [0 ..] (Seq.fromList <$> groupByFibs feedEntries)
        writePage' (pageNumber,pageEntries) = 
            GlobalFeedItem.writePage pageNumber pageEntries 0
        writePagesProgram = 
            runExceptT $
            evalStateT
                (forM_ pages writePage')
                GlobalFeedWriter.emptyGlobalFeedWriterState
        globalFeedCreatedState = 
            execProgram "writeGlobalFeed" writePagesProgram writeState
    in globalFeedCreatedState

fibs :: [Int]
fibs = 
    let acc (a,b) = Just (a + b, (b, a + b))
    in 1 : 1 : unfoldr acc (1, 1)

groupByFibs :: [a] -> [[a]]
groupByFibs as = 
    let acc (_,[]) = Nothing
        acc ([],_) = error "ran out of fibs that should not happen"
        acc (x:xs,ys) = Just (take x ys, (xs, drop x ys))
    in unfoldr acc (fibs, as)

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
globalStreamTests
    :: [TestTree]
globalStreamTests = 
    let getEventTypes start maxItems direction =
          evalProgram "ReadAllStream" (runExceptT $ P.toListM $ Streams.globalEventsProducer direction start >-> P.take maxItems) programState
        programState = pagedTestStateItems 29
    in [ testCase "Past End of the feed backward" $
         assertEqual
             "Should return error"
             (Left
                  (EventStoreActionErrorInvalidGlobalFeedPosition
                       (GlobalFeedPosition
                        { globalFeedPositionPage = 6
                        , globalFeedPositionOffset = 9
                        })))
             (getEventTypes
                  (Just $ GlobalFeedPosition 6 9)
                  3
                  QueryDirectionBackward)
       ]

addUpIops :: Seq LogEvent -> Map (IopsCategory, IopsOperation, Text) Int
addUpIops = foldr' acc Map.empty
  where
    acc (LogEventIops category operation ProgramId{..} i) s = 
        let normalizedProgramId = T.filter isAlpha unProgramId
            key = (category, operation, normalizedProgramId)
        in Map.alter (appendValue i) key s
    acc _ s = s
    appendValue a Nothing = Just a
    appendValue a (Just b) = Just (a + b)

whenIndexing1000ItemsIopsIsMinimal :: Assertion
whenIndexing1000ItemsIopsIsMinimal = 
    let afterIndexState = 
            execProgramUntilIdle
                "indexer"
                globalFeedWriterProgram
                (testStateItems 100)
        afterReadState = 
            execProgramUntilIdle
                "globalFeedReader"
                (pageThroughGlobalFeed 10)
                afterIndexState
        expectedWriteState = Map.fromList [
           ((UnpagedRead,IopsScanUnpaged,"indexer"),138)
          ,((TableRead,IopsGetItem,"collectAncestorsThread"),1)
          ,((TableRead,IopsGetItem,"globalFeedReader"),2)
          ,((TableRead,IopsGetItem,"verifyPagesThread"),583)
          ,((TableRead,IopsGetItem,"writeItemsToPageThread"),1)
          ,((TableRead,IopsQuery,"collectAncestorsThread"),100)
          ,((TableRead,IopsQuery,"globalFeedReader"),100)
          ,((TableRead,IopsQuery,"writeEvents"),99)
          ,((TableWrite,IopsWrite,"verifyItemsThread"),100)
          ,((TableWrite,IopsWrite,"verifyPagesThread"),2)
          ,((TableWrite,IopsWrite,"writeEvents"),100)
          ,((TableWrite,IopsWrite,"writeItemsToPageThread"),1)]
    in assertEqual
           "Should be small iops"
           expectedWriteState
           (addUpIops (afterReadState ^. testStateLog))

errorThrownIfTryingToWriteAnEventInAMultipleGap :: Assertion
errorThrownIfTryingToWriteAnEventInAMultipleGap = 
    let streamId = "MyStream"
        multiWriteEventData = 
            WriteEventData
            { writeEventDataStreamId = StreamId streamId
            , writeEventDataExpectedVersion = Just (-1)
            , writeEventDataEvents = sampleEventEntry :| [secondSampleEventEntry]
            }
        subsequentWriteEventData = 
            WriteEventData
            { writeEventDataStreamId = StreamId streamId
            , writeEventDataExpectedVersion = Just (-1)
            , writeEventDataEvents = sampleEventEntry :| []
            }
        result = 
            evalProgram
                "writeEvents"
                (postEventRequestProgram multiWriteEventData >>
                 postEventRequestProgram subsequentWriteEventData)
                emptyTestState
    in assertEqual "Should return failure" (Right EventExists) result

postTwoEventWithTheSameEventId :: DynamoCmdM Queue (Either EventStoreActionError EventWriteResult)
postTwoEventWithTheSameEventId = 
    let postEventRequest = 
            WriteEventData
            { writeEventDataStreamId = StreamId "MyStream"
            , writeEventDataExpectedVersion = Nothing
            , writeEventDataEvents = sampleEventEntry :| []
            }
    in postEventRequestProgram postEventRequest >>
       postEventRequestProgram postEventRequest

subsequentWriteWithSameEventIdReturnsSuccess :: Assertion
subsequentWriteWithSameEventIdReturnsSuccess = 
    let result = 
            evalProgram "test" postTwoEventWithTheSameEventId emptyTestState
    in assertEqual "Should return success" (Right WriteSuccess) result

subsequentWriteWithSameEventIdDoesNotAppendSecondEvent :: Assertion
subsequentWriteWithSameEventIdDoesNotAppendSecondEvent = 
    let program = 
            postTwoEventWithTheSameEventId >>
            runExceptT (getStreamRecordedEvents "MyStream")
        result = evalProgram "test" program emptyTestState
    in assertEqual "Should return a single event" (Right 1) (length <$> result)

subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents :: Assertion
subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents = 
    let streamId = "MyStream"
        (eventId1:eventId2:_) = sampleEventIds
        postEvents events = 
            postEventRequestProgram
                WriteEventData
                { writeEventDataStreamId = StreamId streamId
                , writeEventDataExpectedVersion = Nothing
                , writeEventDataEvents = events
                }
        postDoubleEvents = 
            postEvents $
            sampleEventEntry
            { eventEntryEventId = eventId1
            } :|
            [ sampleEventEntry
              { eventEntryEventId = eventId2
              }]
        postSubsequentEvent = 
            postEvents $
            sampleEventEntry
            { eventEntryEventId = eventId2
            } :|
            []
        program = 
            postDoubleEvents >> postSubsequentEvent >>
            runExceptT (getStreamRecordedEvents "MyStream")
        result = evalProgram "test" program emptyTestState
    in assertEqual
           "Should return the first two events"
           (Right 2)
           (length <$> result)

subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect :: Assertion
subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect = 
    let streamId = "MyStream"
        postEventRequest = 
            WriteEventData
            { writeEventDataStreamId = StreamId streamId
            , writeEventDataExpectedVersion = Nothing
            , writeEventDataEvents = sampleEventEntry :| []
            }
        secondPost = 
            postEventRequest
            { writeEventDataExpectedVersion = Just 0
            }
        program = 
            postEventRequestProgram postEventRequest >>
            postEventRequestProgram secondPost >>
            runExceptT (getStreamRecordedEvents streamId)
        result = evalProgram "test" program emptyTestState
    in assertEqual "Should return two events" (Right 2) (length <$> result)

tests :: [TestTree]
tests = 
    [ testProperty
          "Can round trip FeedEntry via JSON"
          (\(a :: FeedEntry) -> 
                (Aeson.decode . Aeson.encode) a === Just a)
    , testProperty
          "Single event is indexed correctly"
          prop_SingleEventIsIndexedCorrectly
    , testProperty
          "Global Feed preserves stream order"
          prop_EventShouldAppearInGlobalFeedInStreamOrder
    , testProperty
          "Each event appears in it's correct stream"
          prop_EventsShouldAppearInTheirSteamsInOrder
    , testProperty
          "No Write Request can cause a fatal error in global feed writer"
          prop_NoWriteRequestCanCausesAFatalErrorInGlobalFeedWriter
    , testProperty
          "Conflicting writes will not succeed"
          prop_ConflictingWritesWillNotSucceed
    , testProperty
          "All Events can be read individually"
          prop_AllEventsCanBeReadIndividually
    , testProperty "Scan unpaged should be empty" prop_ScanUnpagedShouldBeEmpty
    , testProperty
          "Can read any section of a stream forward"
          prop_CanReadAnySectionOfAStreamForward
    , testProperty
          "Can read any section of a stream backward"
          prop_CanReadAnySectionOfAStreamBackward
    , 
      --testProperty "Moving next then previous returns the initial page in reverse order" todo,
      --testProperty "Moving previous then next returns the initial page in reverse order" todo,
      --testProperty "The result of multiple writers matches what they see" todo,
      --testProperty "Get stream items contains event lists without duplicates or gaps" todo,
      testCase
          "Unit - Subsequent write with same event id returns success"
          subsequentWriteWithSameEventIdReturnsSuccess
    , testCase
          "Unit - Subsequent write with same event id does not append event - multi event"
          subsequentWriteWithSameEventIdDoesNotAppendSecondEventWhenFirstWriteHadMultipleEvents
    , testCase
          "Unit - Subsequent write with same event id does not append event"
          subsequentWriteWithSameEventIdDoesNotAppendSecondEvent
    , testCase
          "Unit - Subsequent write with same event id accepted if expected version is correct"
          subsequentWriteWithSameEventIdAcceptedIfExpectedVersionIsCorrect
    , testCase
          "Unit - Written Events Appear In Read Stream - explicit expected version" $
      writtenEventsAppearInReadStream writeEventsWithExplicitExpectedVersions
    , testCase
          "Unit - Written Events Appear In Read Stream - explicit expected version" $
      writtenEventsAppearInReadStream writeEventsWithNoExpectedVersions
    , testCase
          "Unit - Cannot write event if previous one does not exist"
          cannotWriteEventsOutOfOrder
    , testCase "Unit - Can write first event" canWriteFirstEvent
    , testCase "Unit - Check Iops usage" whenIndexing1000ItemsIopsIsMinimal
    , testCase
          "Unit - Error thrown if trying to write an event in a multiple gap"
          errorThrownIfTryingToWriteAnEventInAMultipleGap
    , testCase
          "Unit - EventNumbers are calculated when there are multiple events"
          eventNumbersCorrectForMultipleEvents
    , testGroup "Global Stream Tests" globalStreamTests]
