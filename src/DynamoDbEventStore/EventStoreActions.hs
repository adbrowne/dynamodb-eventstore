{-# LANGUAGE OverloadedStrings   #-}
module DynamoDbEventStore.EventStoreActions where

import           Control.Lens
import           Control.Monad (forM_)
import           Pipes
import qualified Pipes.Prelude as P
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Monoid
import           Data.Maybe (fromJust)
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

postEventRequestProgram :: PostEventRequest -> DynamoCmdM EventWriteResult
postEventRequestProgram (PostEventRequest sId ev ed et) = do
  let dynamoKey = DynamoKey (Constants.streamDynamoKeyPrefix <> sId) ev
  let values = HM.singleton fieldBody (set avB (Just (BL.toStrict ed)) attributeValue) & 
               HM.insert fieldEventType (set avS (Just et) attributeValue) & 
               HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue)
  writeResult <- GlobalFeedWriter.dynamoWriteWithRetry dynamoKey values 0 
  return $ toEventResult writeResult
  where
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
