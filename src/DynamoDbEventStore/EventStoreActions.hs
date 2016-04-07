{-# LANGUAGE OverloadedStrings   #-}
module DynamoDbEventStore.EventStoreActions(
  ReadStreamRequest(..), 
  ReadAllRequest(..), 
  PostEventRequest(..), 
  EventStoreAction(..),
  EventWriteResult(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadAllRequestProgram) where

import           Control.Lens
import           Safe
import           Control.Monad (forM_)
import           Pipes
import qualified Pipes.Prelude as P
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Monoid
import           Data.Maybe (fromJust, isJust)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           DynamoDbEventStore.EventStoreCommands
import qualified Data.HashMap.Strict     as HM
import           Network.AWS.DynamoDB
import           Text.Printf (printf)
import qualified DynamoDbEventStore.Constants as Constants
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import qualified Data.Aeson as Aeson
import qualified Test.QuickCheck as QC

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadAll ReadAllRequest |
  SubscribeAll SubscribeAllRequest deriving (Show)

data SubscribeAllRequest = SubscribeAllRequest {
} deriving (Show)

data SubscribeAllResponse = SubscribeAllResponse {
} deriving (Show)

data PostEventRequest = PostEventRequest {
   perStreamId        :: T.Text,
   perExpectedVersion :: Maybe Int64,
   perEventData       :: BL.ByteString,
   perEventType       :: T.Text
} deriving (Show)

instance QC.Arbitrary PostEventRequest where
  arbitrary = PostEventRequest <$> (T.pack <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> (TL.encodeUtf8 .  TL.pack <$> QC.arbitrary)
                               <*> (T.pack <$> QC.arbitrary)

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: T.Text,
   rsrStartEventNumber :: Maybe Int64
} deriving (Show)

data ReadAllRequest = ReadAllRequest deriving (Show)

fieldEventType :: T.Text
fieldEventType = "EventType"

fieldBody :: T.Text
fieldBody = "Body"

data EventWriteResult = WriteSuccess | WrongExpectedVersion | EventExists | WriteError deriving (Eq, Show)

ensurePreviousEventExists :: DynamoKey -> DynamoCmdM Bool
ensurePreviousEventExists (DynamoKey _streamId (0)) = return True
ensurePreviousEventExists (DynamoKey streamId eventNumber) = do
  let previousDynamoKey = DynamoKey streamId (eventNumber - 1)
  result <- readFromDynamo' previousDynamoKey
  return $ isJust result 

postEventRequestProgram :: PostEventRequest -> DynamoCmdM EventWriteResult
postEventRequestProgram (PostEventRequest sId ev ed et) = do
  dynamoKeyOrError <- getDynamoKey sId ev
  case dynamoKeyOrError of Left a -> return a
                           Right dynamoKey -> writeMyEvent dynamoKey
  where
    writeMyEvent :: DynamoKey -> DynamoCmdM EventWriteResult
    writeMyEvent dynamoKey = do
      let values = HM.singleton fieldBody (set avB (Just (BL.toStrict ed)) attributeValue) & 
                   HM.insert fieldEventType (set avS (Just et) attributeValue) & 
                   HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue)
      writeResult <- GlobalFeedWriter.dynamoWriteWithRetry dynamoKey values 0 
      return $ toEventResult writeResult
    getDynamoKey :: T.Text -> Maybe Int64 -> DynamoCmdM (Either EventWriteResult DynamoKey)
    getDynamoKey streamId Nothing = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      readResults <- queryBackward' dynamoHashKey 1 Nothing
      let lastEvent = headMay readResults
      return $ Right $ DynamoKey dynamoHashKey $ maybe 0 (\(DynamoReadResult (DynamoKey _key eventNumber) _version _values) -> eventNumber + 1) lastEvent
    getDynamoKey streamId (Just expectedVersion) = do
      let eventNumber = expectedVersion + 1
      let key = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId) eventNumber
      previousEventExists <- ensurePreviousEventExists key
      if previousEventExists then 
        return $ Right key
      else 
        return $ Left WrongExpectedVersion
    toEventResult :: DynamoWriteResult -> EventWriteResult
    toEventResult DynamoWriteSuccess = WriteSuccess
    toEventResult DynamoWriteFailure = WriteError
    toEventResult DynamoWriteWrongVersion = EventExists

getReadStreamRequestProgram :: ReadStreamRequest -> DynamoCmdM [RecordedEvent]
getReadStreamRequestProgram (ReadStreamRequest sId startEventNumber) = do
  readResults <- queryBackward' (Constants.streamDynamoKeyPrefix <> sId) 10 startEventNumber
  return $ fmap toRecordedEvent readResults
  where 
    toRecordedEvent :: DynamoReadResult -> RecordedEvent
    toRecordedEvent (DynamoReadResult key _version values) = fromJust $ do
      eventType <- view (ix fieldEventType . avS) values 
      eventBody <- view (ix fieldBody . avB) values 
      return $ RecordedEvent sId (dynamoKeyEventNumber key) eventBody eventType

getPageDynamoKey :: Int -> DynamoKey 
getPageDynamoKey pageNumber =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

feedEntryToEventKey :: GlobalFeedWriter.FeedEntry -> EventKey
feedEntryToEventKey GlobalFeedWriter.FeedEntry { GlobalFeedWriter.feedEntryStream = streamId, GlobalFeedWriter.feedEntryNumber = eventNumber } = 
  EventKey (streamId, eventNumber)

readPageKeys :: DynamoReadResult -> [EventKey]
readPageKeys (DynamoReadResult _key _version values) = fromJust $ do
   body <- view (ix Constants.pageBodyKey . avB) values 
   feedEntries <- Aeson.decodeStrict body
   return $ fmap feedEntryToEventKey feedEntries

getPagesAfter :: Int -> Producer EventKey DynamoCmdM ()
getPagesAfter startPage = do
  result <- lift $ readFromDynamo' (getPageDynamoKey startPage)
  case result of (Just entries) -> forM_ (readPageKeys entries) yield >> getPagesAfter (startPage + 1)
                 Nothing        -> return ()

getReadAllRequestProgram :: ReadAllRequest -> DynamoCmdM [EventKey]
getReadAllRequestProgram ReadAllRequest = P.toListM (getPagesAfter 0)
