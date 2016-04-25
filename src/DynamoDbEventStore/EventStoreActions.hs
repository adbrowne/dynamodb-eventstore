{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module DynamoDbEventStore.EventStoreActions(
  ReadStreamRequest(..), 
  ReadAllRequest(..), 
  PostEventRequest(..), 
  EventType(..),
  EventEntry(..),
  EventStoreAction(..),
  EventWriteResult(..),
  postEventRequestProgram,
  getReadStreamRequestProgram,
  getReadAllRequestProgram) where

import           Control.Monad.Except
import           BasicPrelude
import           Control.Lens hiding ((.=))
import           Safe
import           TextShow
import           Pipes
import qualified Pipes.Prelude as P
import qualified Data.ByteString.Lazy as BL
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
import qualified Data.Aeson as Aeson
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

newtype EventType = EventType T.Text deriving (Show, Eq, Ord, IsString)

eventTypeToText :: EventType -> T.Text
eventTypeToText (EventType t) = t

data EventEntry = EventEntry {
  eventEntryData :: BL.ByteString,
  eventEntryType :: EventType
} deriving (Show, Eq, Ord, Generic)

instance Serialize.Serialize EventEntry

instance Serialize.Serialize EventType where
  put (EventType t) = (Serialize.put . T.encodeUtf8) t
  get = EventType . T.decodeUtf8 <$> Serialize.get 

data PostEventRequest = PostEventRequest {
   perStreamId        :: T.Text,
   perExpectedVersion :: Maybe Int64,
   perEvents          :: [EventEntry]
} deriving (Show)

instance QC.Arbitrary EventEntry where
  arbitrary = EventEntry <$> (TL.encodeUtf8 .  TL.pack <$> QC.arbitrary)
                         <*> (EventType . T.pack <$> QC.arbitrary)

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

type UserProgramStack = ExceptT Text DynamoCmdM

ensureExpectedVersion :: DynamoKey -> UserProgramStack Bool
ensureExpectedVersion (DynamoKey _streamId (-1)) = return True
ensureExpectedVersion (DynamoKey streamId expectedEventNumber) = do
  result <- queryBackward' streamId 1 (Just expectedEventNumber)
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
                           Right dynamoKey -> lift $ writeMyEvent dynamoKey
  where
    writeMyEvent :: DynamoKey -> DynamoCmdM EventWriteResult
    writeMyEvent dynamoKey = do
      let values = HM.singleton fieldBody (set avB (Just (Serialize.encode eventEntries)) attributeValue) & 
                   HM.insert Constants.needsPagingKey (set avS (Just "True") attributeValue) &
                   HM.insert Constants.eventCountKey (set avN (Just ((showt . length) eventEntries)) attributeValue)
      writeResult <- GlobalFeedWriter.dynamoWriteWithRetry dynamoKey values 0 
      return $ toEventResult writeResult
    dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber
    getDynamoKey :: T.Text -> Maybe Int64 -> UserProgramStack (Either EventWriteResult DynamoKey)
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
      let firstEventNumber = dynamoKeyEventNumber key
      let eventEntriesWithEventNumber = zip [firstEventNumber..] eventEntries
      let recordedEvents = fmap (\(eventNumber, EventEntry {..}) -> RecordedEvent sId eventNumber (BL.toStrict eventEntryData) (eventTypeToText eventEntryType)) eventEntriesWithEventNumber
      return $ reverse recordedEvents

getPageDynamoKey :: Int -> DynamoKey 
getPageDynamoKey pageNumber =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

feedEntryToEventKeys :: GlobalFeedWriter.FeedEntry -> [EventKey]
feedEntryToEventKeys GlobalFeedWriter.FeedEntry { GlobalFeedWriter.feedEntryStream = streamId, GlobalFeedWriter.feedEntryNumber = eventNumber, GlobalFeedWriter.feedEntryCount = entryCount } = 
  (\number -> EventKey(streamId, number)) <$> (take entryCount [eventNumber..])

fromJustError :: String -> Maybe a -> a
fromJustError msg Nothing  = error msg
fromJustError _   (Just x) = x

readPageKeys :: DynamoReadResult -> [EventKey]
readPageKeys (DynamoReadResult _key _version values) = fromJustError "fromJust readPageKeys" $ do
   body <- view (ix Constants.pageBodyKey . avB) values 
   feedEntries <- Aeson.decodeStrict body
   return $ feedEntries >>= feedEntryToEventKeys

getPagesAfter :: Int -> Producer EventKey DynamoCmdM ()
getPagesAfter startPage = do
  result <- lift $ readFromDynamo' (getPageDynamoKey startPage)
  case result of (Just entries) -> forM_ (readPageKeys entries) yield >> getPagesAfter (startPage + 1)
                 Nothing        -> return ()

getReadAllRequestProgram :: ReadAllRequest -> DynamoCmdM [EventKey]
getReadAllRequestProgram ReadAllRequest = P.toListM (getPagesAfter 0)
