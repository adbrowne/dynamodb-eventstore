{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE OverloadedStrings          #-}

module DynamoDbEventStore.StreamEntry (
  StreamEntry(..)
  ,dynamoReadResultToStreamEntry
  , eventTypeToText
  , unEventTime
  , streamEntryToValues
  , EventType(..)
  , EventEntry(..)
  , EventTime(..)) where

import BasicPrelude
import Control.Lens
import qualified Data.Text                    as T
import qualified Data.HashMap.Lazy                     as HM
import Control.Monad.Except
import           Data.List.NonEmpty                    (NonEmpty (..))
import qualified Data.List.NonEmpty                    as NonEmpty
import qualified Data.Serialize                        as Serialize
import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.Text.Lazy                        as TL
import qualified Data.Text.Lazy.Encoding               as TL
import           Data.Time.Calendar (Day)
import qualified Data.ByteString.Lazy                  as BL
import           GHC.Generics
import           DynamoDbEventStore.AmazonkaImplementation (readFieldGeneric)
import           Network.AWS.DynamoDB (AttributeValue,avB,avN,avS,attributeValue)
import           TextShow (showt)
import qualified Test.QuickCheck                       as QC
import           Test.QuickCheck.Instances()

import DynamoDbEventStore.Types (StreamId(..),DynamoReadResult(..),DynamoKey(..), EventId(..),EventStoreActionError(..),DynamoValues)
import qualified DynamoDbEventStore.Constants          as Constants

newtype EventType = EventType Text deriving (Show, Eq, Ord, IsString)
instance Serialize.Serialize EventType where
  put (EventType t) = (Serialize.put . encodeUtf8) t
  get = EventType . decodeUtf8 <$> Serialize.get

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

instance QC.Arbitrary EventTime where
  arbitrary =
    EventTime <$> QC.arbitrary

newtype SecondPrecisionUtcTime = SecondPrecisionUtcTime UTCTime

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

newtype NonEmptyWrapper a = NonEmptyWrapper (NonEmpty a)
instance Serialize.Serialize a => Serialize.Serialize (NonEmptyWrapper a) where
  put (NonEmptyWrapper xs) = Serialize.put (NonEmpty.toList xs)
  get = do
    xs <- Serialize.get
    maybe (fail "NonEmptyWrapper: found an empty list") (return . NonEmptyWrapper) (NonEmpty.nonEmpty xs)

instance Serialize.Serialize EventTime where
  put (EventTime t) = (Serialize.put . formatTime defaultTimeLocale "%s%Q") t
  get = do
    textValue <- Serialize.get
    time <- parseTimeM False defaultTimeLocale "%s%Q" textValue
    return $ EventTime time

data StreamEntry = StreamEntry {
  streamEntryStreamId :: StreamId,
  streamEntryFirstEventNumber :: Int64,
  streamEntryEventEntries :: NonEmpty EventEntry,
  streamEntryNeedsPaging :: Bool }

binaryDeserialize :: (MonadError EventStoreActionError m, Serialize.Serialize a) => DynamoKey -> ByteString -> m a
binaryDeserialize key x = do
  let value = Serialize.decode x
  case value of Left err    -> throwError (EventStoreActionErrorBodyDecode key err)
                Right v     -> return v

readField :: (MonadError EventStoreActionError m) => Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField =
   readFieldGeneric EventStoreActionErrorFieldMissing

valuesIsPaged :: DynamoValues -> Bool
valuesIsPaged values =
    values &
    HM.member Constants.needsPagingKey &
    not

dynamoReadResultToStreamEntry :: MonadError EventStoreActionError m => DynamoReadResult -> m StreamEntry
dynamoReadResultToStreamEntry (DynamoReadResult key@(DynamoKey dynamoHashKey firstEventNumber) _version values) = do
  eventBody <- readField Constants.pageBodyKey avB values
  let streamId = T.drop (T.length Constants.streamDynamoKeyPrefix) dynamoHashKey
  NonEmptyWrapper eventEntries <- binaryDeserialize key eventBody
  let entryIsPaged = valuesIsPaged values
  let streamEvent = StreamEntry {
        streamEntryStreamId = StreamId streamId,
        streamEntryFirstEventNumber = firstEventNumber,
        streamEntryEventEntries = eventEntries,
        streamEntryNeedsPaging = not entryIsPaged }
  return streamEvent

streamEntryToValues :: StreamEntry -> DynamoValues
streamEntryToValues StreamEntry{..} = 
    let
      bodyValue = set avB (Just ((Serialize.encode . NonEmptyWrapper) streamEntryEventEntries)) attributeValue
      eventCountValue = set avN (Just ((showt . length) streamEntryEventEntries)) attributeValue
      needsPagingValue = set avS (Just "True") attributeValue
    in HM.singleton Constants.pageBodyKey bodyValue &
       HM.insert Constants.needsPagingKey needsPagingValue &
       HM.insert Constants.eventCountKey eventCountValue
