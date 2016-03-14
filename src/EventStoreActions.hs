{-# LANGUAGE OverloadedStrings   #-}
module EventStoreActions where

import           Control.Monad
import           Control.Lens
import qualified Data.ByteString.Lazy as BL
import           Data.Function
import           Data.Int
import           Data.Monoid
import qualified Data.List as L
import           Data.Maybe (fromMaybe, fromJust)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import           EventStoreCommands
import qualified Data.HashMap.Strict     as HM
import           Network.AWS.DynamoDB
import           Text.Printf (printf)
import qualified DynamoDbEventStore.Constants as Constants
import qualified GlobalFeedWriter
import           GlobalFeedWriter (FeedEntry())
import qualified Data.Aeson as Aeson

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

fieldEventType :: T.Text
fieldEventType = "EventType"

fieldBody :: T.Text
fieldBody = "Body"

postEventRequestProgram :: PostEventRequest -> EventStoreCmdM EventWriteResult
postEventRequestProgram (PostEventRequest sId ev ed et) = do
  let eventKey = EventKey (StreamId sId,ev)
  let strictED = BL.toStrict ed
  writeEvent' eventKey et strictED

postEventRequestProgramNew :: PostEventRequest -> DynamoCmdM EventWriteResult
postEventRequestProgramNew (PostEventRequest sId ev ed et) = do
  let eventKey = DynamoKey sId ev
  let values = HM.singleton fieldBody (set avB (Just (BL.toStrict ed)) attributeValue) & HM.insert fieldEventType (set avS (Just et) attributeValue) & HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue)

  writeResult <- writeToDynamo' eventKey values 0 
  return $ toEventResult writeResult
  where
    toEventResult :: DynamoWriteResult -> EventWriteResult
    toEventResult DynamoWriteSuccess = WriteSuccess
    toEventResult DynamoWriteFailure = WriteError
    toEventResult DynamoWriteWrongVersion = EventExists

getReadStreamRequestProgram :: ReadStreamRequest -> EventStoreCmdM [RecordedEvent]
getReadStreamRequestProgram (ReadStreamRequest sId) = do
  getEventsBackward' (StreamId sId) 10 Nothing

getReadStreamRequestProgramNew :: ReadStreamRequest -> DynamoCmdM [RecordedEvent]
getReadStreamRequestProgramNew (ReadStreamRequest sId) = do
  readResults <- queryBackward' sId 10 Nothing
  return $ fmap toRecordedEvent readResults
  where 
    toRecordedEvent :: DynamoReadResult -> RecordedEvent
    toRecordedEvent (DynamoReadResult key version values) = fromJust $ do
      eventType <- view (ix fieldEventType . avS) values 
      eventBody <- view (ix fieldBody . avB) values 
      return $ RecordedEvent (dynamoKeyKey key) (dynamoKeyEventNumber key) eventBody eventType

getReadAllRequestProgram :: ReadAllRequest -> EventStoreCmdM [EventKey]
getReadAllRequestProgram ReadAllRequest = do
  result <- getPageEntry' (0,0)
  return $ fromMaybe [] $ fmap snd result

getPageDynamoKey :: Int -> DynamoKey 
getPageDynamoKey pageNumber =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

feedEntryToEventKey :: GlobalFeedWriter.FeedEntry -> EventKey
feedEntryToEventKey GlobalFeedWriter.FeedEntry { GlobalFeedWriter.feedEntryStream = streamId, GlobalFeedWriter.feedEntryNumber = eventNumber } = 
  EventKey (StreamId streamId, eventNumber)

readPageKeys :: DynamoReadResult -> [EventKey]
readPageKeys (DynamoReadResult _key _version values) = fromJust $ do
   body <- view (ix Constants.pageBodyKey . avB) values 
   feedEntries <- Aeson.decodeStrict body
   return $ fmap feedEntryToEventKey feedEntries

getReadAllRequestProgramNew :: ReadAllRequest -> DynamoCmdM [EventKey]
getReadAllRequestProgramNew ReadAllRequest = do
  result <- readFromDynamo' (getPageDynamoKey 0)
  return $ maybe [] readPageKeys result

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

--newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
--type EventReadResult = Maybe (EventType, BS.ByteString, Maybe PageKey)
previousEventIsPaged :: EventKey -> EventStoreCmdM Bool
previousEventIsPaged (EventKey (_, 0)) = return True
previousEventIsPaged key = do
  storedEvent <- getEvent' (getPrevKey key)
  return $ isPaged storedEvent
    where
      isPaged Nothing = False
      isPaged (Just(_,_,Just _)) = True
      isPaged (Just(_,_,Nothing)) = False
      getPrevKey (EventKey (s, n)) = EventKey (s, n - 1)

writePagesProgram :: Maybe Int -> EventStoreCmdM ()
writePagesProgram Nothing = return ()
writePagesProgram (Just 0) = return ()
writePagesProgram (Just i) = do
  unpagedEvents <- scanUnpagedEvents'
  processEvents unpagedEvents
  writePagesProgram (Just $ i - 1)
    where
      processEvents [] = wait'
      processEvents unpagedEvents = do
        let sortedEvents = (L.reverse . (L.sortBy (compare `on` getEventNumber))) unpagedEvents
        canPageEvents' <- filterM previousEventIsPaged sortedEvents
        mapM_ writeEventToPage canPageEvents'
      getEventNumber (EventKey(_,eventNumber)) = eventNumber
