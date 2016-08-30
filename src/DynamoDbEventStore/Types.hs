{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
module DynamoDbEventStore.Types
  (DynamoKey(..)
  , DynamoValues
  , DynamoReadResult(..)
  , DynamoWriteResult (..)
  , DynamoVersion
  , LogLevel(..)
  , QueryDirection(..)
  , StreamId(..)
  , PageKey(..)
  , FeedEntry(..)
  , RecordedEvent(..)
  , EventId(..)
  , EventKey(..)
  , ValueUpdate(..))
where

import           BasicPrelude
import           Network.AWS.DynamoDB
import           Data.Time.Clock
import           Data.Aeson
import qualified Data.Serialize            as Serialize
import           GHC.Generics
import qualified Data.UUID                 as UUID
import qualified Test.QuickCheck           as QC
import qualified Data.HashMap.Strict       as HM
import           TextShow.TH

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

newtype StreamId = StreamId { unStreamId :: Text } deriving (Ord, Eq, Show, Hashable)
deriveTextShow ''StreamId

instance QC.Arbitrary StreamId where
  arbitrary = StreamId . fromString <$> QC.arbitrary

newtype PageKey = PageKey { unPageKey :: Int64 } deriving (Ord, Eq, Num, Enum)

instance QC.Arbitrary PageKey where
  arbitrary =
    let
      positiveToPageKey (QC.Positive p) = PageKey p
    in positiveToPageKey <$> QC.arbitrary

instance Show PageKey where
  showsPrec precendence (PageKey p) = showsPrec precendence p

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

instance FromJSON StreamId where
  parseJSON (String v) =
    return $ StreamId v
  parseJSON _ = mzero
instance ToJSON StreamId where
  toJSON (StreamId streamId) =
    String streamId

instance FromJSON FeedEntry where
    parseJSON (Object v) = FeedEntry <$>
                           v .: "s" <*>
                           v .: "n" <*>
                           v .: "c"
    parseJSON _                = mempty

instance ToJSON FeedEntry where
    toJSON (FeedEntry stream number entryCount) =
        object ["s" .= stream, "n" .= number, "c" .=entryCount]

newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
deriveTextShow ''EventKey
data PageStatus = Version Int | Full | Verified deriving (Eq, Show, Generic)

newtype EventId = EventId { unEventId :: UUID.UUID } deriving (Show, Eq, Ord, Generic)

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