{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}

module DynamoDbEventStore.GlobalFeedItem
  (GlobalFeedItem(..)
  ,globalFeedItemsProducer
  ,PageStatus(..)
  ,firstPageKey
  ,readPage
  ,readPageMustExist
  ,writePage
  ,writeGlobalFeedItem
  ,updatePageStatus
  ) where

import BasicPrelude
import Control.Lens
import Control.Monad.Except
import qualified Data.Sequence                         as Seq
import           Text.Printf                           (printf)
import qualified Data.Text                             as T
import qualified Data.Aeson                            as Aeson
import qualified Data.ByteString                       as BS
import qualified Data.ByteString.Lazy                  as BL
import qualified Data.HashMap.Lazy                     as HM
import Pipes (Producer,yield)
import           Data.Either.Combinators (eitherToError)

import qualified DynamoDbEventStore.EventStoreCommands as EventStoreCommands
import DynamoDbEventStore.EventStoreCommands (MonadEsDsl, dynamoWriteWithRetry, readFromDynamo, readExcept, QueryDirection(..),ValueUpdate(..), updateItem)
import DynamoDbEventStore.Types (PageKey(..), DynamoVersion, FeedEntry(..), DynamoKey(..), DynamoWriteResult,EventStoreActionError(..),DynamoReadResult(..), DynamoValues, EventKey(..))
import           Network.AWS.DynamoDB (AttributeValue,avB,avN,avS,attributeValue)

pageDynamoKeyPrefix :: Text
pageDynamoKeyPrefix = "page$"

pageBodyKey :: Text
pageBodyKey = "Body"

pageStatusKey :: Text
pageStatusKey = "Status"

getPageDynamoKey :: PageKey -> DynamoKey
getPageDynamoKey (PageKey pageNumber) =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (pageDynamoKeyPrefix <> paddedPageNumber) 0

itemToJsonByteString :: Aeson.ToJSON a => a -> BS.ByteString
itemToJsonByteString = BL.toStrict . Aeson.encode . Aeson.toJSON

data GlobalFeedItem =
  GlobalFeedItem {
   globalFeedItemPageKey :: PageKey,
   globalFeedItemPageStatus :: PageStatus,
   globalFeedItemVersion :: DynamoVersion,
   globalFeedItemFeedEntries :: Seq FeedEntry }

data PageStatus =
  PageStatusIncomplete
  | PageStatusComplete
  | PageStatusVerified
  deriving (Read, Show, Eq)

readField :: (MonadError EventStoreActionError m) => Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField =
   EventStoreCommands.readField EventStoreActionErrorFieldMissing

jsonByteStringToItem :: (Aeson.FromJSON a, MonadError EventStoreActionError m) => ByteString -> m a
jsonByteStringToItem a = eitherToError $ over _Left EventStoreActionErrorJsonDecodeError $ Aeson.eitherDecodeStrict a

firstPageKey :: PageKey
firstPageKey = PageKey 0

readFeedEntries :: (MonadError EventStoreActionError m) => DynamoValues -> m (Seq FeedEntry)
readFeedEntries values = do
   body <- readField pageBodyKey avB values
   jsonByteStringToItem body

readPageStatus :: (MonadError EventStoreActionError m) => DynamoValues -> m PageStatus
readPageStatus values = do
   pageStatus <- readField pageStatusKey avS values 
   let formatError = EventStoreActionErrorPageStatusFieldFormat
   readExcept formatError pageStatus

readPage :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> m (Maybe GlobalFeedItem)
readPage pageKey = do
  let dynamoKey = getPageDynamoKey pageKey
  result <- readFromDynamo dynamoKey
  maybe (return Nothing) readResult result
  where
    readResult (DynamoReadResult _key version values) = do
      feedEntries <- readFeedEntries values
      pageStatus <- readPageStatus values
      return (Just GlobalFeedItem {
              globalFeedItemFeedEntries = feedEntries,
              globalFeedItemPageKey = pageKey,
              globalFeedItemPageStatus = pageStatus,
              globalFeedItemVersion = version })

readPageMustExist :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> m GlobalFeedItem
readPageMustExist pageKey =
  let
    onError = throwError $ EventStoreActionErrorPageDoesNotExist pageKey
  in do
    readResult <- readPage pageKey
    maybe onError return readResult

globalFeedItemsProducerInternal :: (MonadEsDsl m, MonadError EventStoreActionError m) => (PageKey -> PageKey) -> Maybe PageKey -> Producer GlobalFeedItem m ()
globalFeedItemsProducerInternal _next (Just (PageKey (-1))) = return ()
globalFeedItemsProducerInternal next Nothing = globalFeedItemsProducerInternal next (Just firstPageKey)
globalFeedItemsProducerInternal next (Just startPage) = do
  result <- lift $ readPage startPage
  maybe (return()) yieldAndLoop result
  where
    yieldAndLoop a = do
      yield a
      globalFeedItemsProducerInternal next $ Just (next startPage)

globalFeedItemsProducer :: (MonadError EventStoreActionError m, MonadEsDsl m) => QueryDirection -> Maybe PageKey -> Producer GlobalFeedItem m ()
globalFeedItemsProducer QueryDirectionBackward = globalFeedItemsProducerInternal (\(PageKey p) -> PageKey (p - 1))
globalFeedItemsProducer QueryDirectionForward = globalFeedItemsProducerInternal (\(PageKey p) -> PageKey (p + 1))

writeGlobalFeedItem :: (MonadError EventStoreActionError m, MonadEsDsl m) => GlobalFeedItem -> m DynamoWriteResult
writeGlobalFeedItem GlobalFeedItem{..} =
  writePage globalFeedItemPageKey globalFeedItemFeedEntries globalFeedItemVersion

pageStatusToAttribute :: PageStatus -> AttributeValue
pageStatusToAttribute pageStatus =
  set avS (Just (show pageStatus)) attributeValue

updatePageStatus :: (MonadError EventStoreActionError m, MonadEsDsl m) => PageKey -> PageStatus -> m ()
updatePageStatus pageKey newStatus =
  let
    dynamoKey = getPageDynamoKey pageKey
    changes = HM.singleton pageStatusKey (ValueUpdateSet (pageStatusToAttribute newStatus))
  in
    void $ updateItem dynamoKey changes

writePage :: (MonadError EventStoreActionError m, MonadEsDsl m) => PageKey -> Seq FeedEntry -> DynamoVersion -> m DynamoWriteResult
writePage pageNumber entries version = do
  let feedEntry = itemToJsonByteString entries
  let dynamoKey = getPageDynamoKey pageNumber
  let body =
        HM.singleton pageBodyKey (set avB (Just feedEntry) attributeValue)
        & HM.insert pageStatusKey (pageStatusToAttribute PageStatusComplete)
  dynamoWriteWithRetry dynamoKey body version
