{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards           #-}

module DynamoDbEventStore.DynamoInterpreter where

import           Data.Aeson
import           Data.Time.Clock
import           Control.Exception
import           Control.Concurrent
import           Control.Applicative
import           Data.Monoid
import           Control.Monad.Free
import           Data.Map                (Map)
import           Data.Maybe              (fromJust)
import qualified Data.Map                as M
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text.Lazy          as TL
import qualified Data.Text               as T
import qualified Data.Vector             as V
import           System.Random
import           EventStoreActions
import           EventStoreCommands
import           Aws
import           Aws.Core
import           Aws.DynamoDb.Commands
import           Aws.DynamoDb.Core

fieldStreamId = "streamId"
fieldEventNumber = "eventNumber"
fieldEventType = "eventType"
fieldPageStatus = "pageStatus"
fieldBody = "body"
fieldPageKey = "pageKey"
fieldPagingRequired = "pagingRequired"
fieldEventKeys = "eventKeys"
unpagedIndexName = "unpagedIndex"

getDynamoKeyForEvent :: EventKey -> PrimaryKey
getDynamoKeyForEvent (EventKey (StreamId streamId, evtNumber)) =
  hrk fieldStreamId (toValue streamId) fieldEventNumber (toValue evtNumber)

showText :: Int -> T.Text
showText = T.pack . show

getPagePartitionStreamId :: Int -> Int -> T.Text
getPagePartitionStreamId partition page =
  "$Page-" <> showText partition <> "-" <> showText page

getDynamoKeyForPage :: PageKey -> PrimaryKey
getDynamoKeyForPage (partition, pageNumber) =
  let
    streamId = toValue (getPagePartitionStreamId partition pageNumber)
    eventNumber = toValue (toInteger 1)
  in
    hrk fieldStreamId streamId fieldEventNumber eventNumber

getItemField :: (DynVal b, Ord k) => k -> Map k DValue -> Maybe b
getItemField fieldName item =
  M.lookup fieldName item >>= fromValue

readItemJson :: Ord k => FromJSON b => k -> Map k DValue -> Maybe b
readItemJson fieldName item =
  getItemField fieldName item >>= decodeStrict

encodeStrictJson :: ToJSON s => s -> BS.ByteString
encodeStrictJson value =
  BL.toStrict . encode $ value

attrJson :: ToJSON s => T.Text -> s -> Attribute
attrJson name value =
  attr name (encodeStrictJson value)

runCmd :: T.Text -> EventStoreCmd (IO a) -> IO a
runCmd _ (Wait' n) = n ()
runCmd tn (GetEvent' eventKey n) = do
  let key = getDynamoKeyForEvent eventKey
  let req0 = getItem tn key
  resp0 <- runCommand req0
  n $ getResult resp0
  where
    getResult :: GetItemResponse -> EventReadResult
    getResult r = do
      i <- girItem r
      et <- getItemField fieldEventType i
      b <- getItemField fieldBody i
      let pageKey = readItemJson fieldPageKey i
      return (et, b, pageKey)

runCmd tn (WriteEvent' (EventKey (StreamId streamId, evtNumber)) t d n) =
  catch writeItem exnHandler
    where
      -- todo: this function is not complete
      exnHandler (DdbError { ddbErrCode = ConditionalCheckFailedException }) = n EventExists
      writeItem = do
        time <- getCurrentTime
        let i = item [
                  attrAs text fieldStreamId streamId
                  , attrAs int fieldEventNumber (toInteger evtNumber)
                  , attrAs text fieldEventType t
                  , attrAs text fieldPagingRequired (T.pack $ show time)
                  , attr fieldBody d
                ]
        let conditions = Conditions CondAnd [ Condition fieldEventNumber IsNull ]
        let req0 = putItem tn i
        let req1 = req0 { piExpect = conditions }
        runCommand req1
        n WriteSuccess
runCmd tn (SetEventPage' eventKey pk n) =
  catch setEventPage exnHandler
    where
      -- todo: this function is not complete
      exnHandler (DdbError{}) = n SetEventPageError
      setEventPage = do
        let conditions = Conditions CondAnd [ Condition fieldPageKey IsNull ]
        let key = getDynamoKeyForEvent eventKey
        let pageKeyAttribute = attrJson fieldPageKey pk
        let updatePageKey = au pageKeyAttribute
        let pagingReqAttr = attrAs text fieldPagingRequired "unused"
        let updatePagingRequired = AttributeUpdate { auAttr= pagingReqAttr, auAction = UDelete }
        let req0 = updateItem tn key [updatePageKey, updatePagingRequired]
        let req1 = req0 { uiExpect = conditions }
        res0 <- runCommand req1
        n SetEventPageSuccess
runCmd tn (ScanUnpagedEvents' n) =
  catch scanUnpaged exnHandler
    where
      toEntry :: Item -> EventKey
      toEntry i = fromJust $ do
        streamIdDValue <- M.lookup fieldStreamId i
        streamId <- fromValue streamIdDValue
        eventNumberDValue <- M.lookup fieldEventNumber i
        eventNumber <- fromValue eventNumberDValue
        return (EventKey (StreamId streamId, eventNumber))
      -- todo: this function is not complete
      exnHandler (DdbError{}) = n []
      scanUnpaged = do
        let req0 = scan tn
        let req1 = req0 { sIndex = Just unpagedIndexName }
        res0 <- runCommand req1
        n $ (V.toList . fmap toEntry) (srItems res0)
runCmd tn (GetPageEntry' pageKey n) = do
  let key = getDynamoKeyForPage pageKey
  let req0 = getItem tn key
  resp0 <- runCommand req0
  n $ getResult resp0
  where
    getResult :: GetItemResponse -> Maybe (PageStatus, [EventKey])
    getResult r = do
      i <- girItem r
      pageStatus <- readItemJson fieldPageStatus i
      eventKeys <- readItemJson fieldEventKeys i
      return (pageStatus, eventKeys)
runCmd tn (WritePageEntry' (partition, page)
           PageWriteRequest {..} n) =
  catch writePageEntry exnHandler
    where
      -- todo: this function is not complete
      exnHandler (DdbError { ddbErrCode = ConditionalCheckFailedException }) = n Nothing
      buildConditions Nothing =
        Conditions CondAnd [ Condition fieldStreamId IsNull ]
      buildConditions (Just expectedStatus) =
        Conditions CondAnd [ Condition fieldPageStatus (DEq $ DBinary (encodeStrictJson expectedStatus)) ]
      writePageEntry = do
        let i = item [
                  attrAs text fieldStreamId (getPagePartitionStreamId partition page)
                  , attrAs int fieldEventNumber 1
                  , attrJson fieldPageStatus newStatus
                  , attrJson fieldEventKeys entries
                ]
        let conditions = buildConditions expectedStatus
        let req0 = putItem tn i
        let req1 = req0 { piExpect = conditions }
        res0 <- runCommand req1
        n $ Just newStatus

runTest :: T.Text -> EventStoreCmdM a -> IO a
runTest tableName = iterM $ runCmd tableName

evalProgram :: EventStoreCmdM a -> IO a
evalProgram program = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  let unpagedGlobalSecondary = GlobalSecondaryIndex {
    globalIndexName = unpagedIndexName,
    globalKeySchema = HashOnly fieldPagingRequired,
    globalProjection = ProjectKeysOnly,
    globalProvisionedThroughput = ProvisionedThroughput 1 1 }
  let req0 = createTable tableName
        [AttributeDefinition fieldStreamId AttrString
         , AttributeDefinition fieldEventNumber AttrNumber
         , AttributeDefinition fieldPagingRequired AttrString]
        (HashAndRange fieldStreamId fieldEventNumber)
        (ProvisionedThroughput 1 1)
  resp0 <- runCommand req0 { createGlobalSecondaryIndexes = [unpagedGlobalSecondary] }
  runTest tableName program

runCommand r = do
    cfg <- Aws.baseConfiguration
    let cfg' = DdbConfiguration (Region "127.0.0.1" "us-west-2") HTTP (Just 8000)
    Aws.simpleAws cfg cfg' r
