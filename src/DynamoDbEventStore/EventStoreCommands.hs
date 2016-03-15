{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module DynamoDbEventStore.EventStoreCommands where

import           Control.Monad
import           Control.Monad.Free.Church
import           Control.Monad.Free.TH

import           Data.Aeson
import qualified Data.ByteString       as BS
import           Data.Int
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as T
import           GHC.Generics

import           TextShow.TH
import qualified Data.HashMap.Strict     as HM
import qualified Test.QuickCheck as QC
import           Data.Hashable

import           Network.AWS.DynamoDB

newtype StreamId = StreamId T.Text deriving (Ord, Eq, Show, Hashable)
deriveTextShow ''StreamId

instance QC.Arbitrary StreamId where
  arbitrary = StreamId . T.pack <$> QC.arbitrary
  shrink (StreamId xs) = StreamId . T.pack <$> QC.shrink (T.unpack xs)

newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
deriveTextShow ''EventKey
type EventType = T.Text
type PageKey = (Int, Int) -- (Partition, PageNumber)
data EventWriteResult = WriteSuccess | EventExists | WriteError deriving (Eq, Show)
type EventReadResult = Maybe (EventType, BS.ByteString, Maybe PageKey)
data SetEventPageResult = SetEventPageSuccess | SetEventPageError
data PageStatus = Version Int | Full | Verified deriving (Eq, Show, Generic)

data RecordedEvent = RecordedEvent {
   recordedEventStreamId :: T.Text,
   recordedEventNumber   :: Int64,
   recordedEventData     :: BS.ByteString,
   recordedEventType     :: T.Text
} deriving (Show, Eq, Ord)

instance ToJSON RecordedEvent where
  toJSON RecordedEvent{..} =
    object [ "streamId"    .= recordedEventStreamId
           , "eventNumber" .= recordedEventNumber
           , "eventData" .= T.decodeUtf8 recordedEventData
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
  dynamoKeyKey :: T.Text,
  dynamoKeyEventNumber :: Int64
} deriving (Show, Eq, Ord)

type DynamoValues = HM.HashMap T.Text AttributeValue
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
    T.Text -- Hash Key
    Int -- max events to retrieve
    (Maybe Int64) -- starting event, Nothing means start at head
    ([DynamoReadResult] -> next) |
  ScanNeedsPaging'
    ([DynamoKey] -> next) |
  FatalError'
    T.Text |
  SetPulseStatus'
    Bool
    next |
  Log'
    LogLevel
    T.Text
    next

  deriving (Functor)

makeFree ''DynamoCmd

type DynamoCmdM = F DynamoCmd
