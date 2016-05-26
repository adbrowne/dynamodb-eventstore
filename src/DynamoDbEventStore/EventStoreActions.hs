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
  EventStoreActionResult(..),
  FeedDirection(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadEventRequestProgram,
  getReadAllRequestProgram) where

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
  ReadAll ReadAllRequest |
  SubscribeAll SubscribeAllRequest deriving (Show)

data SubscribeAllRequest = SubscribeAllRequest {
} deriving (Show)

data SubscribeAllResponse = SubscribeAllResponse {
} deriving (Show)

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

instance Serialize.Serialize EventType where
  put (EventType t) = (Serialize.put . encodeUtf8) t
  get = EventType . decodeUtf8 <$> Serialize.get 

data EventStoreActionResult =
  PostEventResult (Either EventStoreActionError EventWriteResult)
  | ReadStreamResult (Either EventStoreActionError [RecordedEvent])
  | ReadAllResult (Either EventStoreActionError [RecordedEvent])
  | ReadEventResult (Either EventStoreActionError (Maybe RecordedEvent))
  | TextResult Text
  deriving (Show)

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

data FeedDirection = FeedDirectionFoward | FeedDirectionBackward
  deriving (Eq, Show)

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: Text,
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
  recordedEvents <- toRecordedEvent readResult
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

--nonEmpty :: Monad m => [a] -> (ExceptT EventStoreActionError m) (NonEmpty a)
--nonEmpty = maybe (throwError DynamoItemFoundWithNoEvents RecordFoundWithNoEvents)

toRecordedEvent :: DynamoReadResult -> (ExceptT EventStoreActionError DynamoCmdM) (NonEmpty RecordedEvent)
toRecordedEvent (DynamoReadResult key@(DynamoKey dynamoHashKey firstEventNumber) _version values) = do
  eventBody <- readField Constants.pageBodyKey avB values 
  let streamId = T.drop (T.length Constants.streamDynamoKeyPrefix) dynamoHashKey
  NonEmptyWrapper eventEntries <- binaryDeserialize key eventBody
  let eventEntriesWithEventNumber = NonEmpty.zip (firstEventNumber :| [firstEventNumber + 1 ..]) eventEntries
  let recordedEvents = fmap (\(eventNumber, EventEntry {..}) -> RecordedEvent streamId eventNumber (BL.toStrict eventEntryData) (eventTypeToText eventEntryType) (unEventTime eventEntryCreated) eventEntryEventId eventEntryIsJson) eventEntriesWithEventNumber
  return $ NonEmpty.reverse recordedEvents 

dynamoReadResultProducer :: StreamId -> Maybe Int64 -> Natural -> Producer DynamoReadResult UserProgramStack ()
dynamoReadResultProducer (StreamId streamId) lastEvent batchSize = do
  (firstBatch :: [DynamoReadResult]) <- lift $ queryTable' QueryDirectionBackward (Constants.streamDynamoKeyPrefix <> streamId) batchSize lastEvent
  yieldResultsAndLoop firstBatch
  where
    yieldResultsAndLoop [] = return ()
    yieldResultsAndLoop [recordedEvent] = do
      yield recordedEvent
      let lastEventNumber = dynamoReadResultToEventNumber recordedEvent
      dynamoReadResultProducer (StreamId streamId) (Just $ lastEventNumber - 1) batchSize
    yieldResultsAndLoop (x:xs) = do
      yield x
      yieldResultsAndLoop xs

readResultToRecordedEventPipe :: Pipe DynamoReadResult RecordedEvent UserProgramStack ()
readResultToRecordedEventPipe = do
  readResult <- await
  (recordedEvents :: NonEmpty RecordedEvent) <- lift $ toRecordedEvent readResult
  forM_ (NonEmpty.toList recordedEvents) yield

recordedEventProducer :: StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent UserProgramStack ()
recordedEventProducer streamId lastEvent batchSize = 
  dynamoReadResultProducer streamId lastEvent batchSize 
    >-> readResultToRecordedEventPipe 
    >-> filterLastEvent lastEvent
  where
    filterLastEvent Nothing = P.filter (const True)
    filterLastEvent (Just v) = P.filter ((<= v) . recordedEventNumber) 

getReadEventRequestProgram :: ReadEventRequest -> DynamoCmdM (Either EventStoreActionError (Maybe RecordedEvent))
getReadEventRequestProgram (ReadEventRequest sId eventNumber) = runExceptT $ do
  (events :: [RecordedEvent]) <- P.toListM $ recordedEventProducer (StreamId sId) (Just $ eventNumber + 1) 1
  return $ find ((== eventNumber) . recordedEventNumber) events

getReadStreamRequestProgram :: ReadStreamRequest -> DynamoCmdM (Either EventStoreActionError [RecordedEvent])
getReadStreamRequestProgram (ReadStreamRequest sId startEventNumber maxItems _direction) = 
  runExceptT $ P.toListM $ recordedEventProducer (StreamId sId) ((+1) <$> startEventNumber) 10 >-> P.take (fromIntegral maxItems)

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
  (events :: [RecordedEvent]) <- P.toListM $ recordedEventProducer streamId (Just $ eventNumber + 1) 1
  return $ find ((== eventNumber) . recordedEventNumber) events

lookupEventKey :: Pipe EventKey RecordedEvent UserProgramStack ()
lookupEventKey = forever $ do
  (eventKey@(EventKey(streamId, eventNumber))) <- await
  (maybeRecordedEvent :: Maybe RecordedEvent) <- lift $ lookupEvent streamId eventNumber
  maybe (throwError $ EventStoreActionErrorCouldNotFindEvent eventKey) yield maybeRecordedEvent

getReadAllRequestProgram :: ReadAllRequest -> DynamoCmdM (Either EventStoreActionError [RecordedEvent])
getReadAllRequestProgram ReadAllRequest = runExceptT $ P.toListM (getPagesAfter 0 >-> lookupEventKey)
