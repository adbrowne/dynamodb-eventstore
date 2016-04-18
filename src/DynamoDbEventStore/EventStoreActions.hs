{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE DeriveGeneric        #-}

module DynamoDbEventStore.EventStoreActions(
  ReadStreamRequest(..), 
  ReadAllRequest(..), 
  PostEventRequest(..), 
  EventEntry(..),
  EventStoreAction(..),
  EventWriteResult(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadAllRequestProgram) where

import           Control.Lens hiding ((.=))
import           Safe
import           Control.Monad (forM_)
import           TextShow
import           Pipes
import qualified Pipes.Prelude as P
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Monoid
import           Data.Maybe (isJust)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           DynamoDbEventStore.EventStoreCommands
import qualified Data.HashMap.Strict     as HM
import           Network.AWS.DynamoDB
import           Text.Printf (printf)
import qualified DynamoDbEventStore.Constants as Constants
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import qualified Test.QuickCheck as QC
import qualified Data.Serialize as Serialize
import           GHC.Generics

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

data EventEntry = EventEntry {
  eventEntryData :: BL.ByteString,
  eventEntryType :: T.Text
} deriving (Show, Eq, Ord, Generic)

instance Serialize.Serialize EventEntry

instance Serialize.Serialize T.Text where
  put = Serialize.put . T.encodeUtf8
  get = T.decodeUtf8 <$> Serialize.get 

data PostEventRequest = PostEventRequest {
   perStreamId        :: T.Text,
   perExpectedVersion :: Maybe Int64,
   perEvents          :: [EventEntry]
} deriving (Show)

instance QC.Arbitrary EventEntry where
  arbitrary = EventEntry <$> (TL.encodeUtf8 .  TL.pack <$> QC.arbitrary)
                         <*> (T.pack <$> QC.arbitrary)

instance QC.Arbitrary PostEventRequest where
  arbitrary = PostEventRequest <$> (T.pack <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> QC.arbitrary

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: T.Text,
   rsrStartEventNumber :: Maybe Int64
} deriving (Show)

data ReadAllRequest = ReadAllRequest deriving (Show)

fieldBody :: T.Text
fieldBody = "Body"

data EventWriteResult = WriteSuccess | WrongExpectedVersion | EventExists | WriteError deriving (Eq, Show)

ensureExpectedVersion :: DynamoKey -> DynamoCmdM Bool
ensureExpectedVersion (DynamoKey _streamId (-1)) = return True
ensureExpectedVersion (DynamoKey streamId eventNumber) = do
  let previousDynamoKey = DynamoKey streamId eventNumber
  result <- readFromDynamo' previousDynamoKey
  return $ isJust result 

postEventRequestProgram :: PostEventRequest -> DynamoCmdM EventWriteResult
postEventRequestProgram (PostEventRequest _sId _ev []) = return WriteSuccess -- todo
postEventRequestProgram (PostEventRequest sId ev eventEntries) = do
  dynamoKeyOrError <- getDynamoKey sId ev (length eventEntries)
  case dynamoKeyOrError of Left a -> return a
                           Right dynamoKey -> writeMyEvent dynamoKey
  where
    writeMyEvent :: DynamoKey -> DynamoCmdM EventWriteResult
    writeMyEvent dynamoKey = do
      let values = HM.singleton fieldBody (set avB (Just (Serialize.encode eventEntries)) attributeValue) & 
                   HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue) &
                   HM.insert Constants.eventCountKey (set avN (Just ((showt . length) eventEntries)) attributeValue)
      writeResult <- GlobalFeedWriter.dynamoWriteWithRetry dynamoKey values 0 
      return $ toEventResult writeResult
    dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber
    getDynamoKey :: T.Text -> Maybe Int64 -> Int -> DynamoCmdM (Either EventWriteResult DynamoKey)
    getDynamoKey streamId Nothing eventCount = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      readResults <- queryBackward' dynamoHashKey 1 Nothing
      let lastEvent = headMay readResults
      let lastEventNumber = maybe (-1) dynamoReadResultToEventNumber lastEvent
      let eventVersion = lastEventNumber + (fromIntegral eventCount)
      return $ Right $ DynamoKey dynamoHashKey eventVersion
    getDynamoKey streamId (Just expectedVersion) eventCount = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      expectedVersionOk <- ensureExpectedVersion $ DynamoKey dynamoHashKey expectedVersion
      if expectedVersionOk then do
        let eventVersion = expectedVersion + (fromIntegral eventCount)
        return $ Right $ DynamoKey dynamoHashKey eventVersion
      else 
        return $ Left WrongExpectedVersion
    toEventResult :: DynamoWriteResult -> EventWriteResult
    toEventResult DynamoWriteSuccess = WriteSuccess
    toEventResult DynamoWriteFailure = WriteError
    toEventResult DynamoWriteWrongVersion = EventExists

readField :: Monoid a => T.Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> Either String a
readField fieldName fieldType values = 
   maybeToEither $ view (ix fieldName . fieldType) values 
   where 
     maybeToEither Nothing  = Left $ "Error reading field: " <> T.unpack fieldName
     maybeToEither (Just x) = Right x

fromEitherError :: String -> Either String a -> a
fromEitherError context  (Left err) = error (context <> " " <> err)
fromEitherError _context (Right a)  = a

getReadStreamRequestProgram :: ReadStreamRequest -> DynamoCmdM [RecordedEvent]
getReadStreamRequestProgram (ReadStreamRequest sId startEventNumber) = do
  readResults <- queryBackward' (Constants.streamDynamoKeyPrefix <> sId) 10 startEventNumber
  return $ readResults >>= toRecordedEvent 
  where 
    toRecordedEvent :: DynamoReadResult -> [RecordedEvent]
    toRecordedEvent (DynamoReadResult key _version values) = fromEitherError "toRecordedEvent" $ do
      eventBody <- readField fieldBody avB values 
      (eventEntries :: [EventEntry]) <- Serialize.decode eventBody
      let recordedEvents = fmap (\EventEntry {..} -> RecordedEvent sId (dynamoKeyEventNumber key) (BL.toStrict eventEntryData) eventEntryType) eventEntries
      return recordedEvents

getPageDynamoKey :: Int -> DynamoKey 
getPageDynamoKey pageNumber =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

feedEntryToEventKey :: GlobalFeedWriter.FeedEntry -> EventKey
feedEntryToEventKey GlobalFeedWriter.FeedEntry { GlobalFeedWriter.feedEntryStream = streamId, GlobalFeedWriter.feedEntryNumber = eventNumber } = 
  EventKey (streamId, eventNumber)

fromJustError :: String -> Maybe a -> a
fromJustError msg Nothing  = error msg
fromJustError _   (Just x) = x

readPageKeys :: DynamoReadResult -> [EventKey]
readPageKeys (DynamoReadResult _key _version values) = fromJustError "fromJust readPageKeys" $ do
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
