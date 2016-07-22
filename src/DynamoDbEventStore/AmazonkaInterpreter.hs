{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module DynamoDbEventStore.AmazonkaInterpreter (runProgram, buildTable, runLocalDynamo, evalProgram, doesTableExist, MyAwsStack, InterpreterError(..)) where

import           BasicPrelude
import           Control.Concurrent                    (threadDelay)
import           Control.Exception.Lens
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.Except
import           Control.Monad.Free.Church
import           Control.Monad.Representable.Reader
import           Control.Monad.Trans.Resource
import qualified Data.HashMap.Strict                   as HM
import           Data.List.NonEmpty                    (NonEmpty (..))
import qualified Data.Text                             as T
import qualified DynamoDbEventStore.Constants          as Constants
import           DynamoDbEventStore.EventStoreCommands hiding (readField)
import qualified DynamoDbEventStore.EventStoreCommands as EventStoreCommands
import           System.Random
import           TextShow

import           Control.Monad.Trans.AWS
import           Network.AWS                           (MonadAWS)
import           Network.AWS.DynamoDB
import           Network.AWS.Waiter

fieldStreamId :: Text
fieldStreamId = "streamId"
fieldEventNumber :: Text
fieldEventNumber = "eventNumber"
fieldVersion :: Text
fieldVersion = "version"
fieldPagingRequired :: Text
fieldPagingRequired = Constants.needsPagingKey
unpagedIndexName :: Text
unpagedIndexName = "unpagedIndex"

getDynamoKeyForEvent :: DynamoKey -> HM.HashMap Text AttributeValue
getDynamoKeyForEvent (DynamoKey hashKey rangeKey) =
    HM.fromList [
        (fieldStreamId, set avS (Just hashKey) attributeValue),
        (fieldEventNumber, set avN (Just (showt rangeKey)) attributeValue)
    ]

itemAttribute :: Text -> Lens' AttributeValue (Maybe v) -> v -> (Text, AttributeValue)
itemAttribute key l value =
  (key, set l (Just value) attributeValue)

readExcept :: (Read a) => (Text -> InterpreterError) -> Text -> Either InterpreterError a
readExcept err t =
  let
    parsed = readMay t
  in case parsed of Nothing  -> Left $ err t
                    (Just a) -> Right a

readField :: (MonadError InterpreterError m) => Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField =
   EventStoreCommands.readField FieldMissing

fromAttributesToDynamoKey :: HM.HashMap Text AttributeValue -> Either InterpreterError DynamoKey
fromAttributesToDynamoKey allValues = do
  streamId <- readField fieldStreamId avS allValues
  eventNumber <- readField fieldEventNumber avN allValues >>= readExcept EventNumberFormat
  return (DynamoKey streamId eventNumber)

toDynamoReadResult :: HM.HashMap Text AttributeValue -> Either InterpreterError DynamoReadResult
toDynamoReadResult allValues = do
  let
    values =
      allValues
        & HM.delete fieldVersion
        & HM.delete fieldStreamId
        & HM.delete fieldEventNumber
  eventKey <- fromAttributesToDynamoKey allValues
  version <- readField fieldVersion avN allValues >>= readExcept EventVersionFormat
  return DynamoReadResult { dynamoReadResultKey = eventKey, dynamoReadResultVersion = version, dynamoReadResultValue = values }

allErrors :: (MonadError InterpreterError m) => [Either InterpreterError a] -> m [a]
allErrors l =
  let
    loop (Left s:_)   _   = throwError s
    loop []             acc = return acc
    loop (Right a:xs) acc = loop xs (a:acc)
  in reverse <$> loop l []

eitherToExcept :: (MonadError e m) => Either e a -> m a
eitherToExcept (Left s) = throwError s
eitherToExcept (Right a) = return a

runCmd :: (Typeable m, MonadCatch m, MonadAWS m, MonadIO m, MonadError InterpreterError m, MonadResource m, MonadReader r m, HasEnv r) => Text -> DynamoCmd (m a) -> m a
runCmd _ (Wait' milliseconds n) = do
  liftIO $ threadDelay (milliseconds * 1000)
  n
runCmd tn (ReadFromDynamo' eventKey n) = do
  let key = getDynamoKeyForEvent eventKey
  let req = getItem tn
            & set giKey key
            & set giConsistentRead (Just True)
  resp <- send req
  result <- getResult resp
  n result
  where
    getResult :: (MonadError InterpreterError m) => GetItemResponse -> m (Maybe DynamoReadResult)
    getResult r =
      let item = view girsItem r
      in
        if item == mempty then return Nothing
        else eitherToExcept (Just <$> toDynamoReadResult item)
runCmd tn (QueryTable' direction streamId limit exclusiveStartKey n) =
  getBackward
    where
      setStartKey Nothing = id
      setStartKey (Just startKey) = set qExclusiveStartKey (HM.fromList [("streamId", set avS (Just streamId) attributeValue),("eventNumber", set avN (Just $ show startKey) attributeValue)])
      scanForward = direction == QueryDirectionForward
      getBackward = do
        resp <- send $
                query tn
                & setStartKey exclusiveStartKey
                & set qConsistentRead (Just True)
                & set qLimit (Just limit)
                & set qScanIndexForward (Just scanForward)
                & set qExpressionAttributeValues (HM.fromList [(":streamId",set avS (Just streamId) attributeValue)])
                & set qKeyConditionExpression (Just $ fieldStreamId <> " = :streamId")
        let items :: [HM.HashMap Text AttributeValue] = view qrsItems resp
        let parsedItems = fmap toDynamoReadResult items
        allErrors parsedItems >>= n
runCmd tn (UpdateItem' DynamoKey { dynamoKeyKey = streamId, dynamoKeyEventNumber = eventNumber } values n) =
  go
  where
    getSetExpressions :: Text -> ValueUpdate -> [Text] -> [Text]
    getSetExpressions _key ValueUpdateDelete xs = xs
    getSetExpressions key (ValueUpdateSet _value) xs =
      let x = key <> "= :" <> key
      in x:xs
    getSetAttributeValues :: Text -> ValueUpdate -> HashMap Text AttributeValue -> HashMap Text AttributeValue
    getSetAttributeValues _key ValueUpdateDelete xs = xs
    getSetAttributeValues key (ValueUpdateSet value) xs =
      HM.insert (":" <> key) value xs
    getRemoveExpressions x ValueUpdateDelete xs = x:xs
    getRemoveExpressions _key (ValueUpdateSet _value) xs = xs
    go = do
        let key = HM.fromList [
                  itemAttribute fieldStreamId avS streamId,
                  itemAttribute fieldEventNumber avN (showt eventNumber) ]
        let setExpressions = HM.foldrWithKey getSetExpressions [] values
        let setExpression =
              if null setExpressions then []
              else ["SET " ++ (T.intercalate ", " setExpressions)]
        let expressionAttributeValues = HM.foldrWithKey getSetAttributeValues HM.empty values
        let removeKeys = HM.foldrWithKey getRemoveExpressions [] values
        let removeExpression =
              if null removeKeys then []
              else ["REMOVE " ++ (T.intercalate ", " removeKeys)]
        let updateExpression = T.intercalate " " (setExpression ++ removeExpression)
        let req0 =
              updateItem tn
              & set uiKey key
              & set uiUpdateExpression (Just updateExpression)
              & if (not . HM.null) expressionAttributeValues then
                 set uiExpressionAttributeValues expressionAttributeValues
                else
                 id
        _ <- send req0
        n True
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
      scanUnpaged = do
        resp <- send $
             scan tn
             & set sIndexName (Just unpagedIndexName)
        allErrors (fmap fromAttributesToDynamoKey (view srsItems resp)) >>= n
runCmd _tn (SetPulseStatus' _ n) = n
runCmd _tn (Log' _level msg n) = do
  liftIO $ print msg
  n -- todo: error "Log' unimplemented"

buildTable :: Text -> MyAwsStack ()
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
         & set ctAttributeDefinitions attributeDefinitions
         & set ctGlobalSecondaryIndexes [unpagedGlobalSecondary]
  _ <- send req0
  _ <- await (tableExists { _waitDelay = 4 }) (describeTable tableName)
  return ()

doesTableExist :: Text -> MyAwsStack Bool
doesTableExist tableName =
  catches describe [handler _ResourceNotFoundException (const $ return False)]
  where
    describe = do
      (resp :: DescribeTableResponse) <- send $ describeTable tableName
      let tableDesc = view drsTable resp
      return $ isJust tableDesc

evalProgram :: DynamoCmdM a -> IO (Either InterpreterError a)
evalProgram program = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = "testtable-" ++ show tableNameId
  _ <- runLocalDynamo $ buildTable tableName
  runLocalDynamo $ runProgram tableName program

runLocalDynamo :: MyAwsStack b -> IO (Either InterpreterError b)
runLocalDynamo x = do
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  env <- newEnv Sydney (FromEnv "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" Nothing)
  runResourceT $ runAWST env $ reconfigure dynamo $ runExceptT x

data InterpreterError =
  FieldMissing Text |
  EventNumberFormat Text |
  EventVersionFormat Text
  deriving (Show, Eq)

type MyAwsStack = (ExceptT InterpreterError) (AWST (ResourceT IO))

runProgram :: Text -> DynamoCmdM a -> MyAwsStack a
runProgram tableName = iterM (runCmd tableName)
