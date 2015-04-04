{-# LANGUAGE TemplateHaskell #-}

module EventStoreActions where

import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Text.Lazy       (Text, pack)
import qualified Data.Text.Lazy       as TL
import           EventStoreCommands

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
} deriving (Show, Eq, Ord)

data SubscribeAllResponse = SubscribeAllResponse {
  events :: [RecordedEvent],
  next   :: Text
} deriving (Show)

data PostEventRequest = PostEventRequest {
   streamId        :: Text,
   expectedVersion :: Int64,
   eventData       :: BL.ByteString
} deriving (Show)

postEventRequestProgram :: PostEventRequest -> EventStoreCmdM EventWriteResult
postEventRequestProgram (PostEventRequest sId ev ed) = do
  let eventKey = EventKey (StreamId (TL.toStrict sId),ev)
  let strictED = BL.toStrict ed
  writeEvent' eventKey ("EventType") strictED

writeEventToPage :: EventKey -> EventStoreCmdM ()
writeEventToPage key = do
  let writeRequest = PageWriteRequest { expectedStatus = Just $ Version 0, newStatus = Version 1, newEntries = [key] }
  ignored <- writePageEntry' (0,0) writeRequest
  return ()

writePagesProgram :: EventStoreCmdM ()
writePagesProgram = do
  unpagedEvents <- scanUnpagedEvents'
  mapM writeEventToPage unpagedEvents
  wait'
  writePagesProgram
