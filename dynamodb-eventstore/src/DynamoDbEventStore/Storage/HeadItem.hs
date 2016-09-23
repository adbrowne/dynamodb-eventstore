{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE FlexibleContexts #-}
module DynamoDbEventStore.Storage.HeadItem (
  getLastFullPage
  ,trySetLastFullPage
  ,trySetLastVerifiedPage
  ,getLastVerifiedPage) where

import BasicPrelude
import Control.Lens (set)
import Control.Monad.Except
import qualified Data.HashMap.Lazy                     as HM
import           Network.AWS.DynamoDB                  (avN,attributeValue)

import DynamoDbEventStore.EventStoreCommands (MonadEsDsl,readExcept,readFromDynamo,readField,writeToDynamo)
import DynamoDbEventStore.Types (EventStoreActionError(..),PageKey(..),DynamoReadResult(..),DynamoReadResult(..),DynamoKey(..))

headDynamoKey :: DynamoKey
headDynamoKey = DynamoKey { dynamoKeyKey = "$head", dynamoKeyEventNumber = 0 }

lastFullPageFieldKey :: Text
lastFullPageFieldKey = "lastFull"

lastVerifiedPageFieldKey ::Text
lastVerifiedPageFieldKey = "lastVerified"

data HeadData = HeadData {
  headDataLastFullPage :: Maybe PageKey,
  headDataLastVerifiedPage :: Maybe PageKey,
  headDataVersion     :: Int }

readPageField :: (MonadError EventStoreActionError m) => DynamoReadResult -> Text -> m PageKey
readPageField DynamoReadResult{..} fieldKey = do
  let missingError = const (EventStoreActionErrorHeadFieldMissing fieldKey)
  fieldValue <- readField missingError fieldKey avN dynamoReadResultValue
  let formatError = EventStoreActionErrorHeadFieldFormat fieldKey
  numberValue <- readExcept formatError fieldValue
  return (PageKey numberValue)

readHeadData :: (MonadEsDsl m, MonadError EventStoreActionError m) => m HeadData
readHeadData = do
  currentHead <- readFromDynamo headDynamoKey
  readHead currentHead
  where
    readHead Nothing = return HeadData {
      headDataLastFullPage = Nothing,
      headDataLastVerifiedPage = Nothing,
      headDataVersion = 0 }
    readHead (Just readResult@DynamoReadResult{..}) = do
      lastFullPage <- readPageField readResult lastFullPageFieldKey
      lastVerifiedPage <- readPageField readResult lastVerifiedPageFieldKey
      return HeadData {
        headDataLastFullPage = Just lastFullPage,
        headDataLastVerifiedPage = Just lastVerifiedPage,
        headDataVersion = dynamoReadResultVersion }

getLastVerifiedPage :: (MonadEsDsl m, MonadError EventStoreActionError m) => m (Maybe PageKey)
getLastVerifiedPage = headDataLastVerifiedPage <$> readHeadData

getLastFullPage :: (MonadEsDsl m, MonadError EventStoreActionError m) =>  m (Maybe PageKey)
getLastFullPage = headDataLastFullPage <$> readHeadData

trySetLastFullPage :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> m ()
trySetLastFullPage latestPage = do
  HeadData{..} <- readHeadData
  when (Just latestPage > headDataLastFullPage) $ do
    let value = HM.singleton lastFullPageFieldKey  (set avN (Just . show $  latestPage) attributeValue)
    void (writeToDynamo headDynamoKey value (headDataVersion + 1))
  return ()

trySetLastVerifiedPage :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> m ()
trySetLastVerifiedPage latestPage = do
  HeadData{..} <- readHeadData
  when (Just latestPage > headDataLastVerifiedPage) $ do
    let value = HM.singleton lastVerifiedPageFieldKey (set avN (Just . show $  latestPage) attributeValue)
    void (writeToDynamo headDynamoKey value (headDataVersion + 1))
  return ()
