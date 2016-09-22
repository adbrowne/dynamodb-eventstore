{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module DynamoDbEventStore.Storage.StreamItem (
    StreamEntry(..)
  , dynamoReadResultToStreamEntry
  , getStreamIdFromDynamoKey
  , eventTypeToText
  , unEventTime
  , streamEntryToValues
  , EventType(..)
  , EventEntry(..)
  , EventTime(..)
  , readStreamProducer
  , dynamoReadResultToEventNumber
  , streamEntryProducer) where

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
import GHC.Natural
import DynamoDbEventStore.EventStoreCommands (MonadEsDsl,queryTable)
--import DynamoDbEventStore.Types (StreamId(..),DynamoReadResult(..),DynamoKey(..),EventStoreActionError)
import           DynamoDbEventStore.AmazonkaImplementation (readFieldGeneric)
import           Network.AWS.DynamoDB (AttributeValue,avB,avN,avS,attributeValue)
import qualified Pipes.Prelude as P
import           TextShow (showt)
import qualified Test.QuickCheck                       as QC
import           Test.QuickCheck.Instances()
import Pipes (Producer,yield,(>->))

import DynamoDbEventStore.Types (StreamId(..),DynamoReadResult(..),DynamoKey(..), EventId(..),EventStoreActionError(..),DynamoValues,QueryDirection)
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

readField :: (MonadError EventStoreActionError m) => DynamoKey -> Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField dynamoKey =
   readFieldGeneric (EventStoreActionErrorFieldMissing dynamoKey)

valuesIsPaged :: DynamoValues -> Bool
valuesIsPaged values =
    values &
    HM.member Constants.needsPagingKey &
    not

streamEntryBodyKey :: Text
streamEntryBodyKey = "Body"

getStreamIdFromDynamoKey :: DynamoKey -> StreamId
getStreamIdFromDynamoKey DynamoKey{..} =
  StreamId $  T.drop (T.length Constants.streamDynamoKeyPrefix) dynamoKeyKey

dynamoReadResultToStreamEntry :: MonadError EventStoreActionError m => DynamoReadResult -> m StreamEntry
dynamoReadResultToStreamEntry (DynamoReadResult key@(DynamoKey _dynamoHashKey firstEventNumber) _version values) = do
  eventBody <- readField key streamEntryBodyKey avB values
  let streamId = getStreamIdFromDynamoKey key
  NonEmptyWrapper eventEntries <- binaryDeserialize key eventBody
  let entryIsPaged = valuesIsPaged values
  let streamEvent = StreamEntry {
        streamEntryStreamId = streamId,
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
    in HM.singleton streamEntryBodyKey bodyValue &
       HM.insert Constants.needsPagingKey needsPagingValue &
       HM.insert Constants.eventCountKey eventCountValue

dynamoReadResultToEventNumber :: DynamoReadResult -> Int64
dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber

readStreamProducer :: (MonadEsDsl m) => QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer DynamoReadResult m ()
readStreamProducer direction (StreamId streamId) startEvent batchSize = do
  (firstBatch :: [DynamoReadResult]) <- lift $ queryTable direction (Constants.streamDynamoKeyPrefix <> streamId) batchSize startEvent
  yieldResultsAndLoop firstBatch
  where
    yieldResultsAndLoop [] = return ()
    yieldResultsAndLoop [readResult] = do
      yield readResult
      let lastEventNumber = dynamoReadResultToEventNumber readResult
      readStreamProducer direction (StreamId streamId) (Just lastEventNumber) batchSize
    yieldResultsAndLoop (x:xs) = do
      yield x
      yieldResultsAndLoop xs

streamEntryProducer :: (MonadEsDsl m, MonadError EventStoreActionError m ) => QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer StreamEntry m ()
streamEntryProducer direction streamId startEvent batchSize =
  let source = readStreamProducer direction streamId startEvent batchSize
  in source >-> P.mapM dynamoReadResultToStreamEntry
