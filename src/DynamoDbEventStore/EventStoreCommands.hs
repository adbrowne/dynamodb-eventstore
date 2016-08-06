{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveFunctor #-}

module DynamoDbEventStore.EventStoreCommands(
  StreamId(..),
  LogLevel(..),
  log',
  writeToDynamo',
  readFromDynamo',
  wait',
  scanNeedsPaging',
  queryTable',
  updateItem',
  setPulseStatus',
  newQueue',
  writeQueue',
  tryReadQueue',
  readField,
  DynamoCmdM,
  DynamoVersion,
  QueryDirection(..),
  RecordedEvent(..),
  EventKey(..),
  EventId(..),
  DynamoCmd(..),
  DynamoKey(..),
  DynamoWriteResult(..),
  DynamoReadResult(..),
  DynamoValues,
  ValueUpdate(..),
  PageKey(..),
  FeedEntry(..)
  ) where
import           BasicPrelude
import           Control.Lens              hiding ((.=))
import           Control.Monad.Base
import           Control.Monad.Except
import           Control.Monad.Free.Church
import           GHC.Natural

import           Data.Aeson
import           GHC.Generics

import qualified Data.HashMap.Strict       as HM
import qualified Data.Serialize            as Serialize
import           Data.Time.Clock
import qualified Data.UUID                 as UUID
import qualified Test.QuickCheck           as QC
import           TextShow.TH

import           Network.AWS.DynamoDB

newtype StreamId = StreamId { unStreamId :: Text } deriving (Ord, Eq, Show, Hashable)
deriveTextShow ''StreamId

instance QC.Arbitrary StreamId where
  arbitrary = StreamId . fromString <$> QC.arbitrary

newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
deriveTextShow ''EventKey
newtype PageKey = PageKey { unPageKey :: Int64 } deriving (Ord, Eq, Num, Enum)

instance Show PageKey where
  showsPrec precendence (PageKey p) = showsPrec precendence p

data PageStatus = Version Int | Full | Verified deriving (Eq, Show, Generic)

newtype EventId = EventId { unEventId :: UUID.UUID } deriving (Show, Eq, Ord, Generic)

instance QC.Arbitrary PageKey where
  arbitrary =
    let
      positiveToPageKey (QC.Positive p) = PageKey p
    in positiveToPageKey <$> QC.arbitrary

instance Serialize.Serialize EventId where
  put (EventId uuid) = do
    let (w0, w1, w2, w3) = UUID.toWords uuid
    Serialize.put w0
    Serialize.put w1
    Serialize.put w2
    Serialize.put w3
  get = EventId <$> do
    w0 <- Serialize.get
    w1 <- Serialize.get
    w2 <- Serialize.get
    w3 <- Serialize.get
    return $ UUID.fromWords w0 w1 w2 w3

instance QC.Arbitrary EventId where
  arbitrary =
    EventId
      <$> (UUID.fromWords
            <$> QC.arbitrary
            <*> QC.arbitrary
            <*> QC.arbitrary
            <*> QC.arbitrary)

data RecordedEvent = RecordedEvent {
   recordedEventStreamId :: Text,
   recordedEventNumber   :: Int64,
   recordedEventData     :: ByteString,
   recordedEventType     :: Text,
   recordedEventCreated  :: UTCTime,
   recordedEventId       :: EventId,
   recordedEventIsJson   :: Bool
} deriving (Show, Eq, Ord)

instance ToJSON RecordedEvent where
  toJSON RecordedEvent{..} =
    object [ "streamId"    .= recordedEventStreamId
           , "eventNumber" .= recordedEventNumber
           , "eventData" .= decodeUtf8 recordedEventData
           , "eventType" .= recordedEventType
           ]

instance FromJSON PageStatus
instance ToJSON PageStatus

instance FromJSON StreamId where
  parseJSON (String v) =
    return $ StreamId v
  parseJSON _ = mzero
instance ToJSON StreamId where
  toJSON (StreamId streamId) =
    String streamId
instance FromJSON EventKey where
  parseJSON (Object v) =
    EventKey <$>
    ((,) <$> v .: "streamId"
         <*> v .: "eventNumber")
  parseJSON _ = mzero
instance ToJSON EventKey where
  toJSON (EventKey(streamId, eventNumber)) =
    object [ "streamId"    .= streamId
           , "eventNumber" .= eventNumber
           ]

data FeedEntry = FeedEntry {
  feedEntryStream :: StreamId,
  feedEntryNumber :: Int64,
  feedEntryCount  :: Int
} deriving (Eq, Show)

instance QC.Arbitrary FeedEntry where
  arbitrary =
    FeedEntry <$> QC.arbitrary
              <*> QC.arbitrary
              <*> QC.arbitrary

instance FromJSON FeedEntry where
    parseJSON (Object v) = FeedEntry <$>
                           v .: "s" <*>
                           v .: "n" <*>
                           v .: "c"
    parseJSON _                = mempty

instance ToJSON FeedEntry where
    toJSON (FeedEntry stream number entryCount) =
        object ["s" .= stream, "n" .= number, "c" .=entryCount]

data DynamoKey = DynamoKey {
  dynamoKeyKey         :: Text,
  dynamoKeyEventNumber :: Int64
} deriving (Show, Eq, Ord)

type DynamoValues = HM.HashMap Text AttributeValue
data DynamoReadResult = DynamoReadResult {
  dynamoReadResultKey     :: DynamoKey,
  dynamoReadResultVersion :: Int,
  dynamoReadResultValue   :: DynamoValues
} deriving (Show, Eq)

type DynamoVersion = Int

data DynamoWriteResult =
  DynamoWriteSuccess |
  DynamoWriteFailure |
  DynamoWriteWrongVersion deriving (Eq, Show)

data LogLevel =
  Debug |
  Info |
  Warn |
  Error

data ValueUpdate =
  ValueUpdateSet AttributeValue
  | ValueUpdateDelete

data QueryDirection =
  QueryDirectionForward
  | QueryDirectionBackward
  deriving (Show, Eq)

data DynamoCmd q next where
  ReadFromDynamo' :: DynamoKey -> (Maybe DynamoReadResult -> next) -> DynamoCmd q next
  WriteToDynamo' :: DynamoKey -> DynamoValues -> DynamoVersion -> (DynamoWriteResult -> next) -> DynamoCmd q next
  QueryTable' :: QueryDirection -> Text -> Natural -> Maybe Int64 -> ([DynamoReadResult] -> next) -> DynamoCmd q next
  UpdateItem' :: DynamoKey -> (HashMap Text ValueUpdate) -> (Bool -> next) -> DynamoCmd q next
  ScanNeedsPaging' :: ([DynamoKey] -> next) -> DynamoCmd q next
  NewQueue' :: (q a -> next) -> DynamoCmd q next
  WriteQueue' :: q a -> a -> next -> DynamoCmd q next
  TryReadQueue' :: q a ->  (Maybe a -> next) -> DynamoCmd q next
  Wait' :: Int -> next -> DynamoCmd q next
  SetPulseStatus' :: Bool -> next -> DynamoCmd q next
  Log' :: LogLevel -> Text -> next -> DynamoCmd q next

deriving instance Functor (DynamoCmd q)

newQueue' :: (MonadFree (DynamoCmd q) m) => m (q a)
newQueue' = liftF $ NewQueue' id

writeQueue' :: (MonadFree (DynamoCmd q) m) => q a -> a -> m ()
writeQueue' queue item = liftF $ WriteQueue' queue item ()

tryReadQueue' :: (MonadFree (DynamoCmd q) m) => q a -> m (Maybe a)
tryReadQueue' queue = liftF $ TryReadQueue' queue id

wait' :: (MonadFree (DynamoCmd q) m) => Int -> m ()
wait' seconds = liftF $ Wait' seconds ()

setPulseStatus' :: (MonadFree (DynamoCmd q) m) => Bool -> m ()
setPulseStatus' active = liftF $ SetPulseStatus' active ()

log' :: (MonadFree (DynamoCmd a) m) => LogLevel -> Text -> m ()
log' level message = liftF $ Log' level message ()

scanNeedsPaging' :: (MonadFree (DynamoCmd q) m) => m ([DynamoKey])
scanNeedsPaging' = liftF $ ScanNeedsPaging' id

updateItem' :: (MonadFree (DynamoCmd q) m) => DynamoKey -> (HashMap Text ValueUpdate) -> m (Bool)
updateItem' key updates = liftF $ UpdateItem' key updates id

readFromDynamo' :: (MonadFree (DynamoCmd q) m) => DynamoKey -> m (Maybe DynamoReadResult)
readFromDynamo' key = liftF $ ReadFromDynamo' key id

writeToDynamo' :: (MonadFree (DynamoCmd q) m) => DynamoKey -> DynamoValues -> DynamoVersion -> m (DynamoWriteResult)
writeToDynamo' key values version = liftF $ WriteToDynamo' key values version id

queryTable' :: (MonadFree (DynamoCmd q) m) => QueryDirection -> Text -> Natural -> Maybe Int64 -> m ([DynamoReadResult])
queryTable' direction hashKey maxEvents startEvent = liftF $ QueryTable' direction hashKey maxEvents startEvent id

type DynamoCmdM q = F (DynamoCmd q)

instance MonadBase (F (DynamoCmd q)) (F (DynamoCmd q)) where
  liftBase = id

readField :: (MonadError e m) => (Text -> e) -> Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField toError fieldName fieldType values =
   let fieldValue = values ^? ix fieldName
   in maybeToEither $ fieldValue >>= view fieldType
   where
     maybeToEither Nothing  = throwError $ toError fieldName
     maybeToEither (Just x) = return x
