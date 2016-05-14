{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module DynamoDbEventStore.EventStoreCommands(
  StreamId(..),
  LogLevel(..),
  log',
  writeToDynamo',
  readFromDynamo',
  wait',
  scanNeedsPaging',
  queryBackward',
  setPulseStatus',
  readField,
  DynamoCmdM,
  DynamoVersion,
  RecordedEvent(..),
  EventKey(..),
  EventId(..),
  DynamoCmd(..),
  DynamoKey(..),
  DynamoWriteResult(..),
  EventReadResult,
  DynamoReadResult(..),
  DynamoValues
  ) where
import           BasicPrelude
import           Control.Lens hiding ((.=))
import           Control.Monad.Free.Church
import           Control.Monad.Free.TH
import           Control.Monad.Except
import           GHC.Natural

import           Data.Aeson
import           GHC.Generics

import qualified Data.Serialize as Serialize
import qualified Data.UUID as UUID
import           Data.Time.Clock
import           TextShow.TH
import qualified Data.HashMap.Strict     as HM
import qualified Test.QuickCheck as QC

import           Network.AWS.DynamoDB

newtype StreamId = StreamId Text deriving (Ord, Eq, Show, Hashable)
deriveTextShow ''StreamId

instance QC.Arbitrary StreamId where
  arbitrary = StreamId . fromString <$> QC.arbitrary

newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
deriveTextShow ''EventKey
type EventType = Text
type PageKey = (Int, Int) -- (Partition, PageNumber)
type EventReadResult = Maybe (EventType, ByteString, Maybe PageKey)
data PageStatus = Version Int | Full | Verified deriving (Eq, Show, Generic)

newtype EventId = EventId UUID.UUID deriving (Show, Eq, Ord, Generic)

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

data DynamoKey = DynamoKey {
  dynamoKeyKey :: Text,
  dynamoKeyEventNumber :: Int64
} deriving (Show, Eq, Ord)

type DynamoValues = HM.HashMap Text AttributeValue
data DynamoReadResult = DynamoReadResult {
  dynamoReadResultKey :: DynamoKey,
  dynamoReadResultVersion :: Int,
  dynamoReadResultValue :: DynamoValues
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

data DynamoCmd next =
  ReadFromDynamo'
    DynamoKey
    (Maybe DynamoReadResult -> next) |
  WriteToDynamo'
    DynamoKey
    DynamoValues
    DynamoVersion
    (DynamoWriteResult -> next) |
  QueryBackward'
    Text -- Hash Key
    Natural -- max events to retrieve
    (Maybe Int64) -- starting event, Nothing means start at head
    ([DynamoReadResult] -> next) |
  ScanNeedsPaging'
    ([DynamoKey] -> next) |
  Wait'
    Int
    next |
  SetPulseStatus'
    Bool
    next |
  Log'
    LogLevel
    Text
    next 

  deriving (Functor)

makeFree ''DynamoCmd

type DynamoCmdM = F DynamoCmd

readField :: (MonadError e m) => (Text -> e) -> Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField toError fieldName fieldType values = 
   let fieldValue = values ^? ix fieldName
   in maybeToEither $ fieldValue >>= view fieldType
   where 
     maybeToEither Nothing  = throwError $ toError fieldName
     maybeToEither (Just x) = return x

