{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module DynamoDbEventStore.EventStoreActions(
  ReadStreamRequest(..), 
  ReadEventRequest(..), 
  ReadAllRequest(..), 
  PostEventRequest(..), 
  EventType(..),
  EventTime(..),
  unEventTime,
  EventEntry(..),
  EventStoreAction(..),
  EventWriteResult(..),
  PostEventResult(..),
  ReadStreamResult(..),
  ReadAllResult(..),
  ReadEventResult(..),
  FeedDirection(..),
  StreamResult(..),
  EventStartPosition(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadEventRequestProgram,
  getReadAllRequestProgram,
  recordedEventProducerBackward) where

import           Data.Either.Combinators
import           Control.Monad.Except
import           BasicPrelude
import           Control.Lens hiding ((.=))
import           Safe
import           GHC.Natural
import           TextShow hiding (fromString)
import qualified Pipes.Prelude as P
import           Pipes hiding (ListT, runListT)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Lazy as TL
import qualified Data.Text as T
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Time.Format
import           Data.Time.Calendar
import           Data.Time.Clock
import qualified Data.Text.Lazy.Encoding as TL
import           DynamoDbEventStore.EventStoreCommands hiding (readField)
import qualified DynamoDbEventStore.EventStoreCommands as EventStoreCommands
import qualified Data.HashMap.Strict     as HM
import           Network.AWS.DynamoDB
import           Text.Printf (printf)
import qualified DynamoDbEventStore.Constants as Constants
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           DynamoDbEventStore.GlobalFeedWriter (EventStoreActionError(..))
import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances()
import qualified Data.Aeson as Aeson
import qualified Data.Serialize as Serialize
import           GHC.Generics

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadEvent ReadEventRequest |
  ReadAll ReadAllRequest deriving (Show)

newtype EventType = EventType Text deriving (Show, Eq, Ord, IsString)
newtype EventTime = EventTime UTCTime deriving (Show, Eq, Ord)
unEventTime :: EventTime -> UTCTime
unEventTime (EventTime utcTime) = utcTime

eventTypeToText :: EventType -> Text
eventTypeToText (EventType t) = t

data EventEntry = EventEntry {
  eventEntryData    :: BL.ByteString,
  eventEntryType    :: EventType,
  eventEntryEventId :: EventId,
  eventEntryCreated :: EventTime,
  eventEntryIsJson  :: Bool
} deriving (Show, Eq, Ord, Generic)

instance Serialize.Serialize EventEntry

newtype NonEmptyWrapper a = NonEmptyWrapper (NonEmpty a)
instance Serialize.Serialize a => Serialize.Serialize (NonEmptyWrapper a) where
  put (NonEmptyWrapper xs) = Serialize.put (NonEmpty.toList xs)
  get = do
    xs <- Serialize.get
    maybe (fail "NonEmptyWrapper: found an empty list") (return . NonEmptyWrapper) (NonEmpty.nonEmpty xs)

instance Serialize.Serialize EventTime where
  put (EventTime t) = (Serialize.put . (formatTime defaultTimeLocale "%s%Q")) t
  get = do 
    textValue <- Serialize.get
    time <- parseTimeM False defaultTimeLocale "%s%Q" textValue
    return $ EventTime time

data EventStartPosition = EventStartHead | EventStartPosition Int64 deriving (Show, Eq)

instance Serialize.Serialize EventType where
  put (EventType t) = (Serialize.put . encodeUtf8) t
  get = EventType . decodeUtf8 <$> Serialize.get 

type StreamOffset = (FeedDirection, EventStartPosition, Natural) 

data StreamResult = StreamResult {
    streamResultEvents   :: [RecordedEvent]
  , streamResultFirst    :: Maybe StreamOffset
  , streamResultNext     :: Maybe StreamOffset
  , streamResultPrevious :: Maybe StreamOffset
  , streamResultLast :: Maybe StreamOffset
} deriving Show

newtype PostEventResult = PostEventResult (Either EventStoreActionError EventWriteResult) deriving Show
newtype ReadStreamResult = ReadStreamResult (Either EventStoreActionError (Maybe StreamResult)) deriving Show
newtype ReadAllResult = ReadAllResult (Either EventStoreActionError [RecordedEvent]) deriving Show
newtype ReadEventResult = ReadEventResult (Either EventStoreActionError (Maybe RecordedEvent)) deriving Show

data PostEventRequest = PostEventRequest {
   perStreamId        :: Text,
   perExpectedVersion :: Maybe Int64,
   perEvents          :: NonEmpty EventEntry
} deriving (Show)

newtype SecondPrecisionUtcTime = SecondPrecisionUtcTime UTCTime

instance QC.Arbitrary EventTime where
  arbitrary =
    EventTime <$> QC.arbitrary

instance QC.Arbitrary SecondPrecisionUtcTime where
  arbitrary =
    SecondPrecisionUtcTime <$> (UTCTime
     <$> (QC.arbitrary  :: QC.Gen Day)
     <*> (secondsToDiffTime <$> QC.choose (0, 86400)))
     
instance QC.Arbitrary EventEntry where
  arbitrary = EventEntry <$> (TL.encodeUtf8 . TL.pack <$> QC.arbitrary)
                         <*> (EventType . fromString <$> QC.arbitrary)
                         <*> QC.arbitrary
                         <*> QC.arbitrary
                         <*> QC.arbitrary

instance QC.Arbitrary PostEventRequest where
  arbitrary = PostEventRequest <$> (fromString <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> ((:|) <$> QC.arbitrary <*> QC.arbitrary)

data FeedDirection = FeedDirectionForward | FeedDirectionBackward
  deriving (Eq, Show)

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: StreamId,
   rsrStartEventNumber :: Maybe Int64,
   rsrMaxItems         :: Natural,
   rsrDirection        :: FeedDirection
} deriving (Show)

data ReadEventRequest = ReadEventRequest {
   rerStreamId         :: Text,
   rerEventNumber      :: Int64
} deriving (Show)

data ReadAllRequest = ReadAllRequest deriving (Show)

data EventWriteResult = WriteSuccess | WrongExpectedVersion | EventExists | WriteError deriving (Eq, Show)

type UserProgramStack = ExceptT EventStoreActionError DynamoCmdM

readField :: (MonadError EventStoreActionError m) => Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField = 
   EventStoreCommands.readField EventStoreActionErrorFieldMissing

ensureExpectedVersion :: DynamoKey -> UserProgramStack Bool
ensureExpectedVersion (DynamoKey _streamId (-1)) = return True
ensureExpectedVersion (DynamoKey streamId expectedEventNumber) = do
  result <- queryTable' QueryDirectionBackward streamId 1 (Just $ expectedEventNumber + 1)
  checkEventNumber result
  where 
    checkEventNumber [] = return False
    checkEventNumber ((readResult@(DynamoReadResult (DynamoKey _key eventNumber) _version _values)):_) = do
      eventCount <- GlobalFeedWriter.entryEventCount readResult
      return $ eventNumber + (fromIntegral eventCount) - 1 == expectedEventNumber

dynamoReadResultToEventNumber :: DynamoReadResult -> Int64
dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber

dynamoReadResultToEventId :: DynamoReadResult -> ExceptT EventStoreActionError DynamoCmdM EventId
dynamoReadResultToEventId readResult = do
  recordedEvents <- toRecordedEventBackward readResult
  let lastEvent = NonEmpty.last recordedEvents
  return (recordedEventId lastEvent)

postEventRequestProgram :: PostEventRequest -> DynamoCmdM (Either EventStoreActionError EventWriteResult)
postEventRequestProgram (PostEventRequest sId ev eventEntries) = runExceptT $ do
  let eventId = (eventEntryEventId . NonEmpty.head) eventEntries
  dynamoKeyOrError <- getDynamoKey sId ev eventId
  case dynamoKeyOrError of Left a -> return a
                           Right dynamoKey -> writeMyEvent dynamoKey
  where
    writeMyEvent :: DynamoKey -> ExceptT EventStoreActionError DynamoCmdM EventWriteResult
    writeMyEvent dynamoKey = do
      let values = HM.singleton Constants.pageBodyKey (set avB (Just ((Serialize.encode . NonEmptyWrapper) eventEntries)) attributeValue) & 
                   HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue) &
                   HM.insert Constants.eventCountKey (set avN (Just ((showt . length) eventEntries)) attributeValue)
      writeResult <- GlobalFeedWriter.dynamoWriteWithRetry dynamoKey values 0 
      return $ toEventResult writeResult
    getDynamoKey :: Text -> Maybe Int64 -> EventId -> UserProgramStack (Either EventWriteResult DynamoKey)
    getDynamoKey streamId Nothing eventId = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      readResults <- queryTable' QueryDirectionBackward dynamoHashKey 1 Nothing
      let lastEvent = headMay readResults
      let lastEventNumber = maybe (-1) dynamoReadResultToEventNumber lastEvent
      lastEventIdIsNotTheSame <- maybe (return True) (\x -> (\y -> y /= eventId) <$> dynamoReadResultToEventId x) lastEvent
      if lastEventIdIsNotTheSame then
        let eventVersion = lastEventNumber + 1
        in return $ Right $ DynamoKey dynamoHashKey eventVersion
      else return $ Left WriteSuccess
    getDynamoKey streamId (Just expectedVersion) _eventId = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      expectedVersionOk <- ensureExpectedVersion $ DynamoKey dynamoHashKey expectedVersion
      if expectedVersionOk then do
        let eventVersion = expectedVersion + 1
        return $ Right $ DynamoKey dynamoHashKey eventVersion
      else 
        return $ Left WrongExpectedVersion
    toEventResult :: DynamoWriteResult -> EventWriteResult
    toEventResult DynamoWriteSuccess = WriteSuccess
    toEventResult DynamoWriteFailure = WriteError
    toEventResult DynamoWriteWrongVersion = EventExists

binaryDeserialize :: (MonadError EventStoreActionError m, Serialize.Serialize a) => DynamoKey -> ByteString -> m a
binaryDeserialize key x = do
  let value = Serialize.decode x
  case value of Left err    -> throwError (EventStoreActionErrorBodyDecode key err)
                Right v     -> return v

toRecordedEvent :: DynamoReadResult -> (ExceptT EventStoreActionError DynamoCmdM) (NonEmpty RecordedEvent)
toRecordedEvent (DynamoReadResult key@(DynamoKey dynamoHashKey firstEventNumber) _version values) = do
  eventBody <- readField Constants.pageBodyKey avB values 
  let streamId = T.drop (T.length Constants.streamDynamoKeyPrefix) dynamoHashKey
  NonEmptyWrapper eventEntries <- binaryDeserialize key eventBody
  let eventEntriesWithEventNumber = NonEmpty.zip (firstEventNumber :| [firstEventNumber + 1 ..]) eventEntries
  let buildEvent (eventNumber, EventEntry{..}) = RecordedEvent streamId eventNumber (BL.toStrict eventEntryData) (eventTypeToText eventEntryType) (unEventTime eventEntryCreated) eventEntryEventId eventEntryIsJson
  let recordedEvents = buildEvent <$> eventEntriesWithEventNumber
  return recordedEvents 

toRecordedEventBackward :: DynamoReadResult -> (ExceptT EventStoreActionError DynamoCmdM) (NonEmpty RecordedEvent)
toRecordedEventBackward readResult = NonEmpty.reverse <$> toRecordedEvent readResult

dynamoReadResultProducerBackward :: StreamId -> Maybe Int64 -> Natural -> Producer DynamoReadResult UserProgramStack ()
dynamoReadResultProducerBackward (StreamId streamId) lastEvent batchSize = do
  (firstBatch :: [DynamoReadResult]) <- lift $ queryTable' QueryDirectionBackward (Constants.streamDynamoKeyPrefix <> streamId) batchSize lastEvent
  yieldResultsAndLoop firstBatch
  where
    yieldResultsAndLoop [] = return ()
    yieldResultsAndLoop [readResult] = do
      yield readResult
      let lastEventNumber = dynamoReadResultToEventNumber readResult
      dynamoReadResultProducerBackward (StreamId streamId) (Just $ lastEventNumber) batchSize
    yieldResultsAndLoop (x:xs) = do
      yield x
      yieldResultsAndLoop xs

dynamoReadResultProducerForward :: StreamId -> Maybe Int64 -> Natural -> Producer DynamoReadResult UserProgramStack ()
dynamoReadResultProducerForward (StreamId streamId) firstEvent batchSize = do
  (firstBatch :: [DynamoReadResult]) <- lift $ queryTable' QueryDirectionForward (Constants.streamDynamoKeyPrefix <> streamId) batchSize firstEvent
  yieldResultsAndLoop firstBatch
  where
    yieldResultsAndLoop [] = return ()
    yieldResultsAndLoop [readResult] = do
      yield readResult
      let lastEventNumber = dynamoReadResultToEventNumber readResult
      dynamoReadResultProducerForward (StreamId streamId) (Just $ lastEventNumber) batchSize
    yieldResultsAndLoop (x:xs) = do
      yield x
      yieldResultsAndLoop xs
      
readResultToRecordedEventBackwardPipe :: Pipe DynamoReadResult RecordedEvent UserProgramStack ()
readResultToRecordedEventBackwardPipe = forever $ do
  readResult <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEventBackward readResult
  forM_ (NonEmpty.toList recordedEvents) yield

readResultToRecordedEventPipe :: Pipe DynamoReadResult RecordedEvent UserProgramStack ()
readResultToRecordedEventPipe = forever $ do
  readResult <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEvent readResult
  forM_ (NonEmpty.toList recordedEvents) yield

recordedEventProducerBackward :: StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent UserProgramStack ()
recordedEventProducerBackward streamId lastEvent batchSize = 
  dynamoReadResultProducerBackward streamId ((+1) <$> lastEvent) batchSize 
    >-> readResultToRecordedEventBackwardPipe
    >-> filterLastEvent lastEvent
  where
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber) 

recordedEventProducerForward :: StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent UserProgramStack ()
recordedEventProducerForward streamId Nothing batchSize = 
  dynamoReadResultProducerForward streamId Nothing batchSize >-> readResultToRecordedEventPipe
recordedEventProducerForward streamId firstEvent batchSize = 
  let 
    firstPageBackward = dynamoReadResultProducerBackward streamId ((+1) <$> firstEvent) 1 >-> readResultToRecordedEventPipe 
    restOfPages = dynamoReadResultProducerForward streamId firstEvent batchSize >-> readResultToRecordedEventPipe
  in
    (firstPageBackward >> restOfPages) >-> filterFirstEvent firstEvent
  where
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber) 

getReadEventRequestProgram :: ReadEventRequest -> DynamoCmdM (Either EventStoreActionError (Maybe RecordedEvent))
getReadEventRequestProgram (ReadEventRequest sId eventNumber) = runExceptT $ do
  (events :: [RecordedEvent]) <- P.toListM $ recordedEventProducerBackward (StreamId sId) (Just eventNumber) 1
  return $ find ((== eventNumber) . recordedEventNumber) events

buildStreamResult :: FeedDirection -> Maybe Int64 -> [RecordedEvent] -> Maybe Int64 -> Natural -> Maybe StreamResult
buildStreamResult _ Nothing _ _ _ = Nothing
buildStreamResult FeedDirectionBackward lastEvent events requestedStartEventNumber maxItems = 
  let 
    maxEventNumber = maximum $ recordedEventNumber <$> events
    startEventNumber = fromMaybe maxEventNumber requestedStartEventNumber
    nextEventNumber = startEventNumber - fromIntegral maxItems
  in Just $ StreamResult { 
    streamResultEvents = events, 
    streamResultFirst = Just $ (FeedDirectionBackward, EventStartHead, maxItems),
    streamResultNext = 
      if nextEventNumber > 0 then
        Just $ (FeedDirectionBackward, EventStartPosition nextEventNumber, maxItems)
      else Nothing,
    streamResultPrevious = Just $ (FeedDirectionForward, EventStartPosition (startEventNumber + 1), maxItems), 
    streamResultLast =
      if nextEventNumber > 0 then
        Just $ (FeedDirectionForward, EventStartPosition 0, maxItems) 
      else Nothing 
  }
buildStreamResult FeedDirectionForward lastEvent events requestedStartEventNumber maxItems = 
  let 
    maxEventNumber = maximum $ recordedEventNumber <$> events
    startEventNumber = fromMaybe maxEventNumber requestedStartEventNumber
    nextEventNumber = startEventNumber - fromIntegral maxItems
  in Just $ StreamResult { 
    streamResultEvents = events, 
    streamResultFirst = Just $ (FeedDirectionBackward, EventStartHead, maxItems),
    streamResultNext = 
      if nextEventNumber > 0 then
        Just $ (FeedDirectionBackward, EventStartPosition nextEventNumber, maxItems)
      else Nothing,
    streamResultPrevious = Just $ (FeedDirectionForward, EventStartPosition (startEventNumber + 1), maxItems), 
    streamResultLast =
      if nextEventNumber > 0 then
        Just $ (FeedDirectionForward, EventStartPosition 0, maxItems) 
      else Nothing 
  }

getLastEvent :: StreamId -> UserProgramStack (Maybe Int64)
getLastEvent streamId = do
  lastEvent <- P.toListM $ recordedEventProducerBackward streamId Nothing 1
  return $ 
    case lastEvent of (x:_) -> Just (recordedEventNumber x)
                      _     -> Nothing

getReadStreamRequestProgram :: ReadStreamRequest -> DynamoCmdM (Either EventStoreActionError (Maybe StreamResult))
getReadStreamRequestProgram (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionBackward) = 
  runExceptT $ do
    lastEvent <- getLastEvent streamId
    events <- 
      P.toListM $ 
        recordedEventProducerBackward streamId startEventNumber 10 
          >-> filterLastEvent startEventNumber 
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionBackward lastEvent events startEventNumber maxItems  
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r > (minimumEventNumber v))
    minimumEventNumber start = (fromIntegral start - fromIntegral maxItems)
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber) 
getReadStreamRequestProgram (ReadStreamRequest streamId startEventNumber maxItems FeedDirectionForward) = 
  runExceptT $ do
    lastEvent <- getLastEvent streamId
    events <- 
      P.toListM $ 
        recordedEventProducerForward streamId startEventNumber 10 
          >-> filterFirstEvent startEventNumber 
          >-> maxItemsFilter startEventNumber
    return $ buildStreamResult FeedDirectionForward lastEvent events startEventNumber maxItems  
  where
    maxItemsFilter Nothing = P.take (fromIntegral maxItems)
    maxItemsFilter (Just v) = P.takeWhile (\r -> recordedEventNumber r <= (maximumEventNumber v))
    maximumEventNumber start = (fromIntegral start + fromIntegral maxItems - 1)
    filterFirstEvent Nothing = P.filter (const True)
    filterFirstEvent (Just v) = P.filter ((>= v) . recordedEventNumber) 

getPageDynamoKey :: Int -> DynamoKey 
getPageDynamoKey pageNumber =
  let paddedPageNumber = fromString (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

feedEntryToEventKeys :: GlobalFeedWriter.FeedEntry -> [EventKey]
feedEntryToEventKeys GlobalFeedWriter.FeedEntry { GlobalFeedWriter.feedEntryStream = streamId, GlobalFeedWriter.feedEntryNumber = eventNumber, GlobalFeedWriter.feedEntryCount = entryCount } = 
  (\number -> EventKey(streamId, number)) <$> (take entryCount [eventNumber..])

jsonDecode :: (Aeson.FromJSON a, MonadError EventStoreActionError m) => ByteString -> m a
jsonDecode a = eitherToError $ over _Left EventStoreActionErrorJsonDecodeError $ Aeson.eitherDecodeStrict a

readPageKeys :: DynamoReadResult -> UserProgramStack [EventKey]
readPageKeys (DynamoReadResult _key _version values) = do
   body <- readField Constants.pageBodyKey avB values 
   feedEntries <- jsonDecode body
   return $ feedEntries >>= feedEntryToEventKeys

getPagesAfter :: Int -> Producer EventKey UserProgramStack ()
getPagesAfter startPage = do
  result <- lift $ readFromDynamo' (getPageDynamoKey startPage)
  case result of (Just entries) -> do
                   pageKeys <- lift $ readPageKeys entries
                   forM_ pageKeys yield >> getPagesAfter (startPage + 1)
                 Nothing        -> return ()

lookupEvent :: StreamId -> Int64 -> UserProgramStack (Maybe RecordedEvent)
lookupEvent streamId eventNumber = do
  (events :: [RecordedEvent]) <- P.toListM $ recordedEventProducerBackward streamId (Just eventNumber) 1
  return $ find ((== eventNumber) . recordedEventNumber) events

lookupEventKey :: Pipe EventKey RecordedEvent UserProgramStack ()
lookupEventKey = forever $ do
  (eventKey@(EventKey(streamId, eventNumber))) <- await
  (maybeRecordedEvent :: Maybe RecordedEvent) <- lift $ lookupEvent streamId eventNumber
  maybe (throwError $ EventStoreActionErrorCouldNotFindEvent eventKey) yield maybeRecordedEvent

getReadAllRequestProgram :: ReadAllRequest -> DynamoCmdM (Either EventStoreActionError [RecordedEvent])
getReadAllRequestProgram ReadAllRequest = runExceptT $ P.toListM (getPagesAfter 0 >-> lookupEventKey)
