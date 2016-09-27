{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}

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
  Streams.EventWriteResult(..),
  PostEventResult(..),
  ReadStreamResult(..),
  ReadAllResult(..),
  ReadEventResult(..),
  StreamResult(..),
  StreamOffset,
  GlobalStreamResult(..),
  GlobalStreamOffset,
  EventStartPosition(..),
  GlobalStartPosition(..),
  GlobalFeedPosition(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadEventRequestProgram,
  getReadAllRequestProgram) where

import           BasicPrelude
import           Data.List.NonEmpty                    (NonEmpty (..))
import           DynamoDbEventStore.Paging
import           DynamoDbEventStore.GlobalPaging
import qualified DynamoDbEventStore.Streams as Streams
import           DynamoDbEventStore.Storage.StreamItem (EventEntry(..),EventType(..),EventTime(..),unEventTime)
import           DynamoDbEventStore.EventStoreCommands hiding (readField)
import           DynamoDbEventStore.GlobalFeedWriter   (DynamoCmdWithErrors)
import           DynamoDbEventStore.Types
import qualified Test.QuickCheck                       as QC
import           Test.QuickCheck.Instances             ()

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadEvent ReadEventRequest |
  ReadAll ReadAllRequest deriving (Show)

newtype PostEventResult = PostEventResult (Either EventStoreActionError Streams.EventWriteResult) deriving Show
newtype ReadStreamResult = ReadStreamResult (Either EventStoreActionError (Maybe StreamResult)) deriving Show
newtype ReadAllResult = ReadAllResult (Either EventStoreActionError GlobalStreamResult) deriving Show
newtype ReadEventResult = ReadEventResult (Either EventStoreActionError (Maybe RecordedEvent)) deriving Show

data PostEventRequest = PostEventRequest {
   perStreamId        :: Text,
   perExpectedVersion :: Maybe Int64,
   perEvents          :: NonEmpty EventEntry
} deriving (Show)

instance QC.Arbitrary PostEventRequest where
  arbitrary = PostEventRequest <$> (fromString <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> ((:|) <$> QC.arbitrary <*> QC.arbitrary)

data ReadEventRequest = ReadEventRequest {
   rerStreamId    :: Text,
   rerEventNumber :: Int64
} deriving (Show)

postEventRequestProgram :: (DynamoCmdWithErrors q m) => PostEventRequest -> m Streams.EventWriteResult
postEventRequestProgram (PostEventRequest sId ev eventEntries) =
  Streams.writeEvent (StreamId sId) ev eventEntries

getReadEventRequestProgram :: (DynamoCmdWithErrors q m) => ReadEventRequest -> m (Maybe RecordedEvent)
getReadEventRequestProgram (ReadEventRequest sId eventNumber) =
  Streams.readEvent (StreamId sId) eventNumber 

getReadStreamRequestProgram :: (DynamoCmdWithErrors q m) => ReadStreamRequest -> m (Maybe StreamResult)
getReadStreamRequestProgram request =
  runStreamRequest Streams.streamEventsProducer request

getReadAllRequestProgram :: DynamoCmdWithErrors q m => ReadAllRequest -> m GlobalStreamResult
getReadAllRequestProgram request =
  runGlobalStreamRequest Streams.globalEventsProducer Streams.globalEventKeysProducer request
