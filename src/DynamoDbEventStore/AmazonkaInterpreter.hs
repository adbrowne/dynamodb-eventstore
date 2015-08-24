{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE RankNTypes               #-}

module DynamoDbEventStore.AmazonkaInterpreter where

import           Data.Aeson
import           Data.Time.Clock
import           Control.Exception.Lens
import           Data.Monoid
import           Control.Monad.Free
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Data.Int
import           Data.Map                (Map)
import qualified Data.HashMap.Strict     as HM
import           Control.Lens
import           Data.Maybe              (fromJust)
import qualified Data.Map                as M
import           Data.List.NonEmpty
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text               as T
import qualified Data.Vector             as V
import           TextShow
import           System.Random
import           EventStoreCommands
import           System.IO (stdout)
import qualified Safe
--import           Aws
--import           Aws.Core
--import           Aws.DynamoDb.Commands
--import           Aws.DynamoDb.Core

import Network.AWS
import Network.AWS.DynamoDB
import Control.Monad.Trans.Resource
import Control.Monad.Trans.AWS (newLogger)

fieldStreamId :: T.Text
fieldStreamId = "streamId"
fieldEventNumber :: T.Text
fieldEventNumber = "eventNumber"
fieldEventType :: T.Text
fieldEventType = "eventType"
fieldPageStatus :: T.Text
fieldPageStatus = "pageStatus"
fieldBody :: T.Text
fieldBody = "body"
fieldPageKey :: T.Text
fieldPageKey = "pageKey"
fieldPagingRequired :: T.Text
fieldPagingRequired = "pagingRequired"
fieldEventKeys :: T.Text
fieldEventKeys = "eventKeys"
unpagedIndexName :: T.Text
unpagedIndexName = "unpagedIndex"

getDynamoKey :: T.Text -> Int64 -> HM.HashMap T.Text AttributeValue
getDynamoKey hashKey rangeKey =
    HM.fromList [
        (fieldStreamId, set avS (Just hashKey) attributeValue),
        (fieldEventNumber, set avN (Just (showt rangeKey)) attributeValue)
    ]

getDynamoKeyForEvent :: EventKey -> HM.HashMap T.Text AttributeValue
getDynamoKeyForEvent (EventKey (StreamId streamId, eventNumber)) =
    getDynamoKey streamId eventNumber

showText :: Int -> T.Text
showText = T.pack . show

getPagePartitionStreamId :: Int -> Int -> T.Text
getPagePartitionStreamId partition page =
  "$Page-" <> showText partition <> "-" <> showText page

getDynamoKeyForPage :: PageKey -> HM.HashMap T.Text AttributeValue
getDynamoKeyForPage (partition, pageNumber) =
  let
    hashKey = (getPagePartitionStreamId partition pageNumber)
  in
    getDynamoKey hashKey 1

-- from http://haddock.stackage.org/lts-3.2/basic-prelude-0.5.0/src/BasicPrelude.html#readMay
readMay :: Read a => T.Text -> Maybe a
readMay = Safe.readMay . T.unpack
{- 

getItemField :: (DynVal b, Ord k) => k -> Map k DValue -> Maybe b
getItemField fieldName i =
  M.lookup fieldName i >>= fromValue


attrJson :: ToJSON s => T.Text -> s -> Attribute
attrJson name value =
  attr name (encodeStrictJson value)

-}

itemAttribute :: T.Text -> Lens' AttributeValue (Maybe v) -> v -> (T.Text, AttributeValue)
itemAttribute key lens value =
  (key, set lens (Just value) attributeValue)

readItemJson :: FromJSON b => T.Text -> HM.HashMap T.Text AttributeValue -> Maybe b
readItemJson fieldName i =
  view (ix fieldName . avB) i >>= decodeStrict

attrJson :: ToJSON s => s -> AttributeValue
attrJson value = set avB (Just (encodeStrictJson value)) attributeValue

encodeStrictJson :: ToJSON s => s -> BS.ByteString
encodeStrictJson value =
  BL.toStrict . encode $ value

runCmd :: T.Text -> EventStoreCmd (IO a) -> IO a
runCmd _ (Wait' n) = n ()
runCmd tn (GetEvent' eventKey n) = do
  let key = getDynamoKeyForEvent eventKey
  let req = (getItem tn) & (set giKey key)
  resp <- runCommand req
  n $ getResult resp
  where
    getResult :: GetItemResponse -> EventReadResult
    getResult r = do
      let i = view girsItem r
      et <- view (ix fieldEventType . avS) i
      b <- view (ix fieldBody . avB) i
      let pageKey = readItemJson fieldPageKey i
      return (et, b, pageKey)
runCmd tn (WriteEvent' (EventKey (StreamId streamId, evtNumber)) t d n) =
  catches writeItem [handler _ConditionalCheckFailedException (\_ -> n EventExists)] 
    where
      writeItem = do
        time <- getCurrentTime
        let item = HM.fromList [
                  itemAttribute fieldStreamId avS streamId,
                  itemAttribute fieldEventNumber avN (showt evtNumber),
                  itemAttribute fieldEventType avS t,
                  itemAttribute fieldPagingRequired avS (T.pack $ show time),
                  itemAttribute fieldBody avB d
                ]
        let conditionExpression = Just $ "attribute_not_exists(" <> fieldEventNumber <> ")"
        let req0 =
              putItem tn
              & (set piItem item)
              & (set piConditionExpression conditionExpression)
        _ <- runCommand req0
        n WriteSuccess
runCmd tn (GetEventsBackward' (StreamId streamId) _ _ n) =
  getBackward
    where
      toRecordedEvent :: HM.HashMap T.Text AttributeValue -> RecordedEvent
      toRecordedEvent i = fromJust $ do -- todo: remove fromJust
        sId <- view (ix fieldStreamId . avS) i
        eventNumber <- view (ix fieldEventNumber . avN) i >>= readMay
        et <- view (ix fieldEventType . avS) i
        b <- view (ix fieldBody . avB) i
        return $ RecordedEvent sId eventNumber b et
      getBackward = do
        resp <- runCommand $
                query tn
                & (set qScanIndexForward (Just False))
                & (set qExpressionAttributeValues (HM.fromList [(":streamId",set avS (Just streamId) attributeValue)]))
                & (set qKeyConditionExpression (Just $ fieldStreamId <> " = :streamId"))
        let items :: [HM.HashMap T.Text AttributeValue] = view qrsItems resp
        n $ (fmap toRecordedEvent) items
runCmd tn (ScanUnpagedEvents' n) =
  scanUnpaged
    where
      toEntry :: HM.HashMap T.Text AttributeValue -> EventKey
      toEntry i = fromJust $ do
        streamId <- view (ix fieldStreamId . avS) i
        eventNumber <- view (ix fieldEventNumber . avN) i >>= readMay
        return (EventKey (StreamId streamId, eventNumber))
      scanUnpaged = do
        resp <- runCommand $
             scan tn
             & (set sIndexName $ Just unpagedIndexName)
        n $ fmap toEntry (view srsItems resp)
runCmd tn (SetEventPage' eventKey pk n) =
  setEventPage
    where
      setEventPage = do
        let conditionExpression = Just $ "attribute_not_exists(" <> fieldPageKey <> ")"
        let key = getDynamoKeyForEvent eventKey
        let updateExpression = Just $ "SET " <> fieldPageKey <> "=:pageKey REMOVE " <> fieldPagingRequired
        let expressionAttributeValues = HM.fromList [(":pageKey", attrJson pk)]
        let req0 =
              updateItem tn
              & (set uiKey key)
              & (set uiExpressionAttributeValues expressionAttributeValues)
              & (set uiUpdateExpression updateExpression)
              & (set uiConditionExpression conditionExpression)
        _ <- runCommand req0
        n SetEventPageSuccess
{-
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
      buildConditions (Just expectedStatus') =
        Conditions CondAnd [ Condition fieldPageStatus (DEq $ DBinary (encodeStrictJson expectedStatus')) ]
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
        _ <- runCommand req1
        n $ Just newStatus
-}
runTest :: T.Text -> EventStoreCmdM a -> IO a
runTest tableName = iterM $ runCmd tableName

buildTable :: T.Text -> IO ()
buildTable tableName = do
  let unpagedGlobalSecondary = globalSecondaryIndex
          unpagedIndexName
          (keySchemaElement fieldPagingRequired Hash :| [])
          (set pProjectionType (Just KeysOnly) projection)
          (provisionedThroughput 1 1)
  let attributeDefinitions = [
        attributeDefinition fieldStreamId S,
        attributeDefinition fieldEventNumber N,
        attributeDefinition fieldPagingRequired S ]

  let req0 = createTable tableName
         (keySchemaElement fieldStreamId Hash :| [ keySchemaElement fieldEventNumber Range ])
         (provisionedThroughput 1 1)
         & (set ctAttributeDefinitions attributeDefinitions)
         & (set ctGlobalSecondaryIndexes [unpagedGlobalSecondary])
{-        [AttributeDefinition fieldStreamId AttrString
         , AttributeDefinition fieldEventNumber AttrNumber
         , AttributeDefinition fieldPagingRequired AttrString]
        (HashAndRange fieldStreamId fieldEventNumber)
        (ProvisionedThroughput 1 1) -}
  _ <- runCommand req0 -- { createGlobalSecondaryIndexes = [unpagedGlobalSecondary] }
  return ()

evalProgram :: EventStoreCmdM a -> IO a
evalProgram program = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  buildTable tableName
  runTest tableName program

runProgram :: T.Text -> EventStoreCmdM a -> IO a
runProgram = runTest

redirect :: Maybe (BS.ByteString, Int) -> Endpoint -> Endpoint
redirect Nothing       = id
redirect (Just (h, p)) =
      (endpointHost   .~ h)
    . (endpointPort   .~ p)
    . (endpointSecure .~ (p == 443))

runCommand :: forall a. (AWSRequest a) => a -> IO (Rs a)
runCommand req = do
    myLogger <- newLogger Trace stdout
    env <- newEnv Sydney (FromEnv "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" Nothing)
    runResourceT $ runAWS (env) $ do -- & envLogger .~ myLogger) $ do
      endpoint (redirect  $ Just ("localhost", 8000)) $ do
        send req
