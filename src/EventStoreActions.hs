{-# LANGUAGE TemplateHaskell #-}

module EventStoreActions where

import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Text.Lazy       (Text, pack)
import qualified Data.Text.Lazy       as TL

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  SubscribeAll SubscribeAllRequest deriving (Show)

data SubscribeAllRequest = SubscribeAllRequest {
   from            :: Maybe Text
} deriving (Show)

data RecordedEvent = RecordedEvent {
   recordedEventStreamId :: Text,
   recordedEventNumber   :: Int64,
   recordedEventData     :: BL.ByteString
} deriving (Show)

data SubscribeAllResponse = SubscribeAllResponse {
  events :: [RecordedEvent],
  next   :: Text
} deriving (Show)

data PostEventRequest = PostEventRequest {
   streamId        :: Text,
   expectedVersion :: Int64,
   eventData       :: BL.ByteString
} deriving (Show)
