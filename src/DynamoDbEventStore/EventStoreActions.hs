{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module DynamoDbEventStore.EventStoreActions(
  ReadStreamRequest(..), 
  ReadEventRequest(..), 
  ReadAllRequest(..), 
  PostEventRequest(..), 
  EventType(..),
  EventEntry(..),
  EventStoreAction(..),
  EventWriteResult(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadEventRequestProgram,
  getReadAllRequestProgram) where

import           Data.Either.Combinators
import           Control.Monad.Except
import           BasicPrelude
import           Control.Lens hiding ((.=))
import           Safe
import qualified Data.Text as T
import           TextShow hiding (fromString)
import           Pipes
import qualified Pipes.Prelude as P
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           DynamoDbEventStore.EventStoreCommands
import qualified Data.HashMap.Strict     as HM
import           Network.AWS.DynamoDB
import           Text.Printf (printf)
import qualified DynamoDbEventStore.Constants as Constants
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import qualified Test.QuickCheck as QC
import qualified Data.Aeson as Aeson
import qualified Data.Serialize as Serialize
import           GHC.Generics

-- High level event store actions
-- should map almost one to one with http interface
data EventStoreAction =
  PostEvent PostEventRequest |
  ReadStream ReadStreamRequest |
  ReadEvent ReadEventRequest |
  ReadAll ReadAllRequest |
  SubscribeAll SubscribeAllRequest deriving (Show)

data SubscribeAllRequest = SubscribeAllRequest {
} deriving (Show)

data SubscribeAllResponse = SubscribeAllResponse {
} deriving (Show)

newtype EventType = EventType Text deriving (Show, Eq, Ord, IsString)

eventTypeToText :: EventType -> Text
eventTypeToText (EventType t) = t

data EventEntry = EventEntry {
  eventEntryData :: BL.ByteString,
  eventEntryType :: EventType
} deriving (Show, Eq, Ord, Generic)

instance Serialize.Serialize EventEntry

instance Serialize.Serialize EventType where
  put (EventType t) = (Serialize.put . encodeUtf8) t
  get = EventType . decodeUtf8 <$> Serialize.get 

data PostEventRequest = PostEventRequest {
   perStreamId        :: Text,
   perExpectedVersion :: Maybe Int64,
   perEvents          :: [EventEntry]
} deriving (Show)

instance QC.Arbitrary EventEntry where
  arbitrary = EventEntry <$> (TL.encodeUtf8 . TL.pack <$> QC.arbitrary)
                         <*> (EventType . fromString <$> QC.arbitrary)

instance QC.Arbitrary PostEventRequest where
  arbitrary = PostEventRequest <$> (fromString <$> QC.arbitrary)
                               <*> QC.arbitrary
                               <*> QC.arbitrary

data ReadStreamRequest = ReadStreamRequest {
   rsrStreamId         :: Text,
   rsrStartEventNumber :: Maybe Int64
} deriving (Show)

data ReadEventRequest = ReadEventRequest {
   rerStreamId         :: Text,
   rerEventNumber      :: Int64
} deriving (Show)

data ReadAllRequest = ReadAllRequest deriving (Show)

fieldBody :: Text
fieldBody = "Body"

data EventWriteResult = WriteSuccess | WrongExpectedVersion | EventExists | WriteError deriving (Eq, Show)

type UserProgramStack = ExceptT Text DynamoCmdM

ensureExpectedVersion :: DynamoKey -> UserProgramStack Bool
ensureExpectedVersion (DynamoKey _streamId (-1)) = return True
ensureExpectedVersion (DynamoKey streamId expectedEventNumber) = do
  result <- queryBackward' streamId 1 (Just $ expectedEventNumber + 1)
  checkEventNumber result
  where 
    checkEventNumber [] = return False
    checkEventNumber ((readResult@(DynamoReadResult (DynamoKey _key eventNumber) _version _values)):_) = do
      eventCount <- GlobalFeedWriter.entryEventCount readResult
      return $ eventNumber + (fromIntegral eventCount) - 1 == expectedEventNumber


postEventRequestProgram :: PostEventRequest -> DynamoCmdM (Either Text EventWriteResult)
postEventRequestProgram (PostEventRequest _sId _ev []) = return $ Left "PostRequest must have events"
postEventRequestProgram (PostEventRequest sId ev eventEntries) = runExceptT $ do
  dynamoKeyOrError <- getDynamoKey sId ev
  case dynamoKeyOrError of Left a -> return a
                           Right dynamoKey -> writeMyEvent dynamoKey
  where
    writeMyEvent :: DynamoKey -> ExceptT Text DynamoCmdM EventWriteResult
    writeMyEvent dynamoKey = do
      let values = HM.singleton fieldBody (set avB (Just (Serialize.encode eventEntries)) attributeValue) & 
                   HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue) &
                   HM.insert Constants.eventCountKey (set avN (Just ((showt . length) eventEntries)) attributeValue)
      writeResult <- GlobalFeedWriter.dynamoWriteWithRetry dynamoKey values 0 
      return $ toEventResult writeResult
    dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber
    getDynamoKey :: Text -> Maybe Int64 -> UserProgramStack (Either EventWriteResult DynamoKey)
    getDynamoKey streamId Nothing = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      readResults <- queryBackward' dynamoHashKey 1 Nothing
      let lastEvent = headMay readResults
      let lastEventNumber = maybe (-1) dynamoReadResultToEventNumber lastEvent
      let eventVersion = lastEventNumber + 1
      return $ Right $ DynamoKey dynamoHashKey eventVersion
    getDynamoKey streamId (Just expectedVersion) = do
      let dynamoHashKey = Constants.streamDynamoKeyPrefix <> streamId
      expectedVersionOk <- ensureExpectedVersion $ DynamoKey dynamoHashKey expectedVersion
      if expectedVersionOk then do
        let eventVersion = expectedVersion + 1
        return $ Right $ DynamoKey dynamoHashKey eventVersion
      else 
        return $ Left WrongExpectedVersion
    toEventResult :: DynamoWriteResult -> EventWriteResult
    toEventResult DynamoWriteSuccess = WriteSuccess
    toEventResult DynamoWriteFailure = WriteError
    toEventResult DynamoWriteWrongVersion = EventExists

fromEitherError :: Text -> Either Text a -> a
fromEitherError context  (Left err) = error (T.unpack (context <> " " <> err))
fromEitherError _context (Right a)  = a

toRecordedEvent :: Text -> DynamoReadResult -> [RecordedEvent]
toRecordedEvent sId (DynamoReadResult key _version values) = fromEitherError "toRecordedEvent" $ do
  eventBody <- readField fieldBody avB values 
  (eventEntries :: [EventEntry]) <- over _Left T.pack $ Serialize.decode eventBody
  let firstEventNumber = dynamoKeyEventNumber key
  let eventEntriesWithEventNumber = zip [firstEventNumber..] eventEntries
  let recordedEvents = fmap (\(eventNumber, EventEntry {..}) -> RecordedEvent sId eventNumber (BL.toStrict eventEntryData) (eventTypeToText eventEntryType)) eventEntriesWithEventNumber
  return $ reverse recordedEvents

getReadEventRequestProgram :: ReadEventRequest -> DynamoCmdM (Maybe RecordedEvent)
getReadEventRequestProgram (ReadEventRequest sId eventNumber) = do
  readResults <- queryBackward' (Constants.streamDynamoKeyPrefix <> sId) 1 (Just $ eventNumber + 1)
  let events = readResults >>= toRecordedEvent sId
  return $ find ((== eventNumber) . recordedEventNumber) events

getReadStreamRequestProgram :: ReadStreamRequest -> DynamoCmdM [RecordedEvent]
getReadStreamRequestProgram (ReadStreamRequest sId startEventNumber) = do
  readResults <- queryBackward' (Constants.streamDynamoKeyPrefix <> sId) 10 ((+1) <$> startEventNumber)
  return $ readResults >>= toRecordedEvent sId

getPageDynamoKey :: Int -> DynamoKey 
getPageDynamoKey pageNumber =
  let paddedPageNumber = fromString (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

feedEntryToEventKeys :: GlobalFeedWriter.FeedEntry -> [EventKey]
feedEntryToEventKeys GlobalFeedWriter.FeedEntry { GlobalFeedWriter.feedEntryStream = streamId, GlobalFeedWriter.feedEntryNumber = eventNumber, GlobalFeedWriter.feedEntryCount = entryCount } = 
  (\number -> EventKey(streamId, number)) <$> (take entryCount [eventNumber..])

maybeToException :: (MonadError e m) => e -> Maybe a -> m a
maybeToException err Nothing  = throwError err
maybeToException _   (Just a) = return a

jsonDecode :: (Aeson.FromJSON a, MonadError Text m) => ByteString -> m a
jsonDecode a = eitherToError $ over _Left fromString $ Aeson.eitherDecodeStrict a

readPageKeys :: DynamoReadResult -> UserProgramStack [EventKey]
readPageKeys (DynamoReadResult _key _version values) = do
   body <- maybeToException "Error reading pageBody" $ view (ix Constants.pageBodyKey . avB) values 
   feedEntries <- jsonDecode body
   return $ feedEntries >>= feedEntryToEventKeys

getPagesAfter :: Int -> Producer EventKey UserProgramStack ()
getPagesAfter startPage = do
  result <- lift $ readFromDynamo' (getPageDynamoKey startPage)
  case result of (Just entries) -> do
                   pageKeys <- lift $ readPageKeys entries
                   forM_ pageKeys yield >> getPagesAfter (startPage + 1)
                 Nothing        -> return ()

getReadAllRequestProgram :: ReadAllRequest -> DynamoCmdM (Either Text [EventKey])
getReadAllRequestProgram ReadAllRequest = runExceptT $ P.toListM (getPagesAfter 0)
