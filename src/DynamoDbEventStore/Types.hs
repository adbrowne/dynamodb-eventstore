{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell #-}
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
  , ValueUpdate(..))
where

import           BasicPrelude
import           Network.AWS.DynamoDB
import           Data.Aeson
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
