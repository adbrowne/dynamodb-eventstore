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
  EventEntry(..),
  EventStoreAction(..),
  EventWriteResult(..),
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
import           DynamoDbEventStore
import           DynamoDbEventStore.GlobalPaging
import           DynamoDbEventStore.Paging
import qualified Test.QuickCheck                       as QC
import           Test.QuickCheck.Instances             ()

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadEvent ReadEventRequest |
  ReadAll ReadAllRequest deriving (Show)

newtype PostEventResult = PostEventResult (Either EventStoreError EventWriteResult) deriving Show
newtype ReadStreamResult = ReadStreamResult (Either EventStoreError (Maybe StreamResult)) deriving Show
newtype ReadAllResult = ReadAllResult (Either EventStoreError GlobalStreamResult) deriving Show
newtype ReadEventResult = ReadEventResult (Either EventStoreError (Maybe RecordedEvent)) deriving Show

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

postEventRequestProgram :: PostEventRequest -> EventStore EventWriteResult
postEventRequestProgram (PostEventRequest sId ev eventEntries) =
  writeEvent (StreamId sId) ev eventEntries

getReadEventRequestProgram :: ReadEventRequest -> EventStore (Maybe RecordedEvent)
getReadEventRequestProgram (ReadEventRequest sId eventNumber) =
  readEvent (StreamId sId) eventNumber 

getReadStreamRequestProgram :: ReadStreamRequest -> EventStore (Maybe StreamResult)
getReadStreamRequestProgram =
  runStreamRequest streamEventsProducer

getReadAllRequestProgram :: ReadAllRequest -> EventStore GlobalStreamResult
getReadAllRequestProgram =
  runGlobalStreamRequest globalEventsProducer globalEventKeysProducer
