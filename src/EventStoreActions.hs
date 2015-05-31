{-# LANGUAGE OverloadedStrings   #-}
module EventStoreActions where

import qualified Data.ByteString.Lazy as BL
import           Data.Int
import qualified Data.Text.Lazy       as TL
import qualified Data.Text            as T
import           EventStoreCommands

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  SubscribeAll SubscribeAllRequest deriving (Show)

data SubscribeAllRequest = SubscribeAllRequest {
   from            :: Maybe TL.Text
} deriving (Show)

data SubscribeAllResponse = SubscribeAllResponse {
  sarEvents :: [RecordedEvent],
  sarNext   :: TL.Text
} deriving (Show)

data PostEventRequest = PostEventRequest {
   perStreamId        :: T.Text,
   perExpectedVersion :: Int64,
   perEventData       :: BL.ByteString,
   perEventType       :: T.Text
} deriving (Show)

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId        :: T.Text
} deriving (Show)

postEventRequestProgram :: PostEventRequest -> EventStoreCmdM EventWriteResult
postEventRequestProgram (PostEventRequest sId ev ed et) = do
  let eventKey = EventKey (StreamId sId,ev)
  let strictED = BL.toStrict ed
  writeEvent' eventKey et strictED

getReadStreamRequestProgram :: ReadStreamRequest -> EventStoreCmdM [RecordedEvent]
getReadStreamRequestProgram (ReadStreamRequest sId) = do
  getEventsBackward' (StreamId sId) 10 Nothing

writeEventToPage :: EventKey -> EventStoreCmdM ()
writeEventToPage key = do
  let writeRequest = PageWriteRequest { expectedStatus = Just $ Version 0, newStatus = Version 1, entries = [key] }
  _ <- writePageEntry' (0,0) writeRequest
  return ()

writePagesProgram :: EventStoreCmdM ()
writePagesProgram = do
  unpagedEvents <- scanUnpagedEvents'
  mapM_ writeEventToPage unpagedEvents
  wait'
  writePagesProgram
