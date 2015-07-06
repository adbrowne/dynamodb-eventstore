{-# LANGUAGE OverloadedStrings   #-}
module EventStoreActions where

import qualified Data.ByteString.Lazy as BL
import           Data.Function
import           Data.Int
import qualified Data.List as L
import           Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import           EventStoreCommands

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadAll ReadAllRequest |
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

data ReadAllRequest = ReadAllRequest deriving (Show)

postEventRequestProgram :: PostEventRequest -> EventStoreCmdM EventWriteResult
postEventRequestProgram (PostEventRequest sId ev ed et) = do
  let eventKey = EventKey (StreamId sId,ev)
  let strictED = BL.toStrict ed
  writeEvent' eventKey et strictED

getReadStreamRequestProgram :: ReadStreamRequest -> EventStoreCmdM [RecordedEvent]
getReadStreamRequestProgram (ReadStreamRequest sId) = do
  getEventsBackward' (StreamId sId) 10 Nothing

getReadAllRequestProgram :: ReadAllRequest -> EventStoreCmdM [EventKey]
getReadAllRequestProgram ReadAllRequest = do
  result <- getPageEntry' (0,0)
  return $ fromMaybe [] $ fmap snd result

writeEventToPage :: EventKey -> EventStoreCmdM ()
writeEventToPage key = do
  currentPage <- getPageEntry' (0,0)
  let writeRequest = buildWriteRequest currentPage
  _ <- writePageEntry' (0,0) writeRequest
  _ <- setEventPage' key (0,0)
  return ()
    where
      buildWriteRequest :: Maybe (PageStatus, [EventKey]) -> PageWriteRequest
      buildWriteRequest Nothing = PageWriteRequest { expectedStatus = Nothing, newStatus = Version 0, entries = [key] }
      buildWriteRequest (Just (pageStatus, currentKeys)) = PageWriteRequest { expectedStatus = (Just pageStatus), newStatus = Version 0, entries = key:currentKeys }

writePagesProgram :: Maybe Int -> EventStoreCmdM ()
writePagesProgram Nothing = return ()
writePagesProgram (Just 0) = return ()
writePagesProgram (Just i) = do
  unpagedEvents <- scanUnpagedEvents'
  processEvents unpagedEvents
  writePagesProgram (Just $ i - 1)
    where
      processEvents [] = wait'
      processEvents unpagedEvents =
        let
         -- sorting is enough for now. we aren't yet simulating
         -- eventual consistency in the index
          sortedEvents = (L.reverse (L.sortBy (compare `on` getEventNumber) unpagedEvents))
        in
          mapM_ writeEventToPage sortedEvents
      getEventNumber (EventKey(_,eventNumber)) = eventNumber
