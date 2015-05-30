{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DynamoDbEventStore.DynamoInterpreter where

import           Data.Time.Clock
import           Control.Exception
import           Control.Concurrent
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
--import           Control.Concurrent
--import           Control.Monad
--import           Control.Monad.Catch
--import           Data.Conduit
--import qualified Data.Conduit.List     as C
--import qualified Data.Text             as T
--import           Network.HTTP.Conduit  (withManager)

fieldStreamId = "streamId"
fieldEventNumber = "eventNumber"
fieldEventType = "eventType"
fieldBody = "body"
fieldPageKey = "pageKey"
fieldPagingRequired = "pagingRequired"
unpagedIndexName :: T.Text = "unpagedIndex"

getDynamoKeyForEvent :: EventKey -> PrimaryKey
getDynamoKeyForEvent (EventKey (StreamId streamId, evtNumber)) =
  hrk fieldStreamId (toValue streamId) fieldEventNumber (toValue evtNumber)

readText :: Read a => T.Text -> a
readText  = read . T.unpack

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
      eventTypeDvalue <- M.lookup fieldEventType i
      et <- fromValue eventTypeDvalue
      bodyDValue <- M.lookup fieldBody i
      b <- fromValue bodyDValue
      let pageKeyDValue = M.lookup fieldPageKey i >>= fromValue
      let pageKey = fmap readText $ pageKeyDValue
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
      exnHandler (DdbError statusCode errCode errMsg ) = do
        n SetEventPageError
      setEventPage = do
        let conditions = Conditions CondAnd [ Condition fieldPageKey IsNull ]
        let key = getDynamoKeyForEvent eventKey
        let pageKeyAttribute = attrAs text fieldPageKey (T.pack $ show pk)
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
      exnHandler (DdbError _ _ _ ) = n []
      scanUnpaged = do
        threadDelay 30000
        let req0 = scan tn
        let req1 = req0 { sIndex = Just unpagedIndexName }
        res0 <- runCommand req1
        n $ (V.toList . (fmap toEntry)) (srItems res0)
runCmd _ (GetPageEntry' k n) = error "todo"
runCmd _ (WritePageEntry' k
           PageWriteRequest
           {
              expectedStatus = expectedStatus,
              newStatus = newStatus,
              newEntries = newEntries
           } n) =
  error "todo"

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
    globalProvisionedThroughput = (ProvisionedThroughput 1 1)
  }
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
