{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE RankNTypes               #-}

module DynamoDbEventStore.AmazonkaInterpreter where

import           Control.Concurrent (threadDelay)
import           Control.Monad.IO.Class
import           Data.Int
import           Data.Typeable
import           Data.Aeson
import           Control.Exception.Lens
import           Data.Monoid
import           Control.Monad.Free.Church
import           Control.Monad.Catch
import qualified Data.HashMap.Strict     as HM
import           Control.Lens
import           Data.Maybe              (fromJust, isJust)
import           Data.List.NonEmpty      (NonEmpty (..))
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text               as T
import           TextShow
import qualified DynamoDbEventStore.Constants as Constants
import           System.Random
import           DynamoDbEventStore.EventStoreCommands
import qualified Safe

import Network.AWS
import Network.AWS.Waiter
import Network.AWS.DynamoDB

fieldStreamId :: T.Text
fieldStreamId = "streamId"
fieldEventNumber :: T.Text
fieldEventNumber = "eventNumber"
fieldVersion :: T.Text
fieldVersion = "version"
fieldEventType :: T.Text
fieldEventType = "eventType"
fieldPageStatus :: T.Text
fieldPageStatus = "pageStatus"
fieldBody :: T.Text
fieldBody = "body"
fieldPageKey :: T.Text
fieldPageKey = "pageKey"
fieldPagingRequired :: T.Text
fieldPagingRequired = Constants.needsPagingKey
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

getDynamoKeyForEvent :: DynamoKey -> HM.HashMap T.Text AttributeValue
getDynamoKeyForEvent (DynamoKey streamId eventNumber) =
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
itemAttribute key l value =
  (key, set l (Just value) attributeValue)

readItemJson :: FromJSON b => T.Text -> HM.HashMap T.Text AttributeValue -> Maybe b
readItemJson fieldName i =
  view (ix fieldName . avB) i >>= decodeStrict

attrJson :: ToJSON s => s -> AttributeValue
attrJson value = set avB (Just (encodeStrictJson value)) attributeValue

encodeStrictJson :: ToJSON s => s -> BS.ByteString
encodeStrictJson value =
  BL.toStrict . encode $ value

toDynamoReadResult :: HM.HashMap T.Text AttributeValue -> Maybe DynamoReadResult
toDynamoReadResult allValues = do
  let 
    values = 
      allValues 
        & HM.delete fieldVersion 
        & HM.delete fieldStreamId 
        & HM.delete fieldEventNumber
  streamId <- view (ix fieldStreamId . avS) allValues 
  eventNumber <- view (ix fieldEventNumber . avN) allValues >>= readMay
  let eventKey = DynamoKey streamId eventNumber
  version <- view (ix fieldVersion . avN) allValues >>= readMay
  return DynamoReadResult { dynamoReadResultKey = eventKey, dynamoReadResultVersion = version, dynamoReadResultValue = values }

runCmd :: (Typeable m, MonadCatch m, MonadAWS m, MonadIO m) => T.Text -> DynamoCmd (m a) -> m a
runCmd _ (Wait' milliseconds n) = do
  liftIO $ threadDelay (milliseconds * 1000)
  n
runCmd tn (ReadFromDynamo' eventKey n) = do
  let key = getDynamoKeyForEvent eventKey
  let req = getItem tn & set giKey key
  resp <- send req
  n $ getResult resp
  where
    getResult :: GetItemResponse -> Maybe DynamoReadResult
    getResult r = 
      toDynamoReadResult $ view girsItem r
runCmd tn (QueryBackward' streamId _ _ n) =
  getBackward
    where
      getBackward = do
        resp <- send $
                query tn
                & (set qScanIndexForward (Just False))
                & (set qExpressionAttributeValues (HM.fromList [(":streamId",set avS (Just streamId) attributeValue)]))
                & (set qKeyConditionExpression (Just $ fieldStreamId <> " = :streamId"))
        let items :: [HM.HashMap T.Text AttributeValue] = view qrsItems resp
        n $ (fmap (fromJust . toDynamoReadResult)) items -- todo remove fromJust
runCmd tn (WriteToDynamo' DynamoKey { dynamoKeyKey = streamId, dynamoKeyEventNumber = eventNumber } values version n) = 
  catches writeItem [handler _ConditionalCheckFailedException (\_ -> n DynamoWriteWrongVersion)] 
  where
    addVersionChecks 0 req = 
        req & set piConditionExpression (Just $ "attribute_not_exists(" <> fieldEventNumber <> ")")
    addVersionChecks _ req = 
        req & set piConditionExpression (Just $ fieldVersion <> " = :itemVersion")
        & set piExpressionAttributeValues (HM.singleton ":itemVersion" (set avN (Just (showt (version - 1))) attributeValue))
        
    writeItem = do
        let item = HM.fromList [
                  itemAttribute fieldStreamId avS streamId,
                  itemAttribute fieldEventNumber avN (showt eventNumber),
                  itemAttribute fieldVersion avN (showt version)]
                  <> values 
        let req0 =
              putItem tn
              & set piItem item
              & addVersionChecks version
        _ <- send req0
        n DynamoWriteSuccess
runCmd tn (ScanNeedsPaging' n) =
  scanUnpaged
    where
      toEntry :: HM.HashMap T.Text AttributeValue -> DynamoKey
      toEntry i = fromJust $ do
        streamId <- view (ix fieldStreamId . avS) i
        eventNumber <- view (ix fieldEventNumber . avN) i >>= readMay
        return (DynamoKey streamId eventNumber)
      scanUnpaged = do
        resp <- send $
             scan tn
             & set sIndexName (Just unpagedIndexName)
        n $ fmap toEntry (view srsItems resp)
runCmd _tn (FatalError' _n) = error "FatalError' unimplemented"
runCmd _tn (SetPulseStatus' _ n) = n
runCmd _tn (Log' _level msg n) = do
  liftIO $ print msg
  n -- todo: error "Log' unimplemented"

buildTable :: T.Text -> AWS ()
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
  _ <- send req0
  _ <- await (tableExists { _waitDelay = 4 }) (describeTable tableName)
  return ()

doesTableExist :: T.Text -> AWS Bool
doesTableExist tableName =
  catches describe [handler _ResourceNotFoundException (const $ return False)] 
  where 
    describe = do
      (resp :: DescribeTableResponse) <- send $ describeTable tableName
      let tableDesc = view drsTable resp
      return $ isJust tableDesc

evalProgram :: DynamoCmdM a -> IO a
evalProgram program = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  runLocalDynamo $ buildTable tableName
  runLocalDynamo $ runProgram tableName program

runLocalDynamo :: AWS b -> IO b
runLocalDynamo x = do
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  env <- newEnv Sydney (FromEnv "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" Nothing)
  runResourceT $ runAWS env $ reconfigure dynamo x

runProgram :: T.Text -> DynamoCmdM a -> AWS a
runProgram tableName = iterM (runCmd tableName)

redirect :: Maybe (BS.ByteString, Int) -> Endpoint -> Endpoint
redirect Nothing       = id
redirect (Just (h, p)) =
      (endpointHost   .~ h)
    . (endpointPort   .~ p)
    . (endpointSecure .~ (p == 443))
