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

data RecordedEvent = RecordedEvent {
   recordedEventStreamId :: Text,
   recordedEventNumber   :: Int64,
   recordedEventData     :: ByteString,
   recordedEventType     :: Text
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

readField :: (MonadError Text m, Monoid a) => Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField fieldName fieldType values = 
   maybeToEither $ view (ix fieldName . fieldType) values 
   where 
     maybeToEither Nothing  = throwError $ "Error reading field: " <> fieldName
     maybeToEither (Just x) = return x

