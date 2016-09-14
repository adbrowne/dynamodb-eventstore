{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module DynamoDbEventStore.AmazonkaImplementation
  (readFromDynamoAws
  ,writeToDynamoAws
  ,updateItemAws
  ,queryTableAws
  ,logAws
  ,scanNeedsPagingAws
  ,waitAws
  ,setPulseStatusAws
  ,readFieldGeneric
  ,fieldPagingRequired
  ,buildTable
  ,doesTableExist
  ,evalProgram
  ,runLocalDynamo
  ,runDynamoCloud
  ,runDynamoCloud2
  ,runProgram
  ,newCacheAws
  ,cacheInsertAws
  ,cacheLookupAws
  ,MetricLogsPair(..)
  ,MetricLogs(..)
  ,InterpreterError(..)
  ,MyAwsM(..)
  ,InMemoryCache
  ,runtimeEnvironmentAmazonkaEnv
  ,runtimeEnvironmentMetricLogs
  ,runtimeEnvironmentCompletePageQueue
  ,RuntimeEnvironment(..))
  where

import BasicPrelude
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Exception.Lens
import Control.Lens
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Trans.AWS hiding (LogLevel)
import Control.Monad.Trans.Resource
import qualified Data.Cache.LRU as LRU
import qualified Data.HashMap.Strict as HM
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.Text as T
import qualified DynamoDbEventStore.Constants as Constants
import DynamoDbEventStore.Types
import GHC.Natural
import Network.AWS.DynamoDB
import Network.AWS.Waiter
import System.CPUTime
import System.Random
import TextShow

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
    HM.fromList
        [ (fieldStreamId, set avS (Just hashKey) attributeValue)
        , (fieldEventNumber, set avN (Just (showt rangeKey)) attributeValue)]

data InterpreterError
    = FieldMissing Text
    | EventNumberFormat Text
    | EventVersionFormat Text
    deriving (Show,Eq)

data MetricLogsPair = MetricLogsPair
    { metricLogsPairCount :: IO ()
    , metricLogsPairTimeMs :: Double -> IO ()
    } 

data MetricLogs = MetricLogs
    { metricLogsReadItem :: MetricLogsPair
    , metricLogsWriteItem :: MetricLogsPair
    , metricLogsQuery :: MetricLogsPair
    , metricLogsUpdateItem :: MetricLogsPair
    , metricLogsScan :: MetricLogsPair
    } 

data RuntimeEnvironment = RuntimeEnvironment
    { _runtimeEnvironmentMetricLogs :: MetricLogs
    , _runtimeEnvironmentCompletePageQueue :: TQueue (PageKey, Seq FeedEntry)
    , _runtimeEnvironmentTableName :: Text
    , _runtimeEnvironmentAmazonkaEnv :: Env
    } 

$(makeLenses ''RuntimeEnvironment)

instance HasEnv RuntimeEnvironment where
    environment = runtimeEnvironmentAmazonkaEnv

eitherToExcept
    :: (MonadError e m)
    => Either e a -> m a
eitherToExcept (Left s) = throwError s
eitherToExcept (Right a) = return a

timeAction
    :: (MonadIO m, MonadReader RuntimeEnvironment m)
    => (MetricLogs -> MetricLogsPair) -> m a -> m a
timeAction getPair action = do
    MetricLogsPair{..} <- views runtimeEnvironmentMetricLogs getPair
    startTime <- liftIO getCPUTime
    a <- action
    endTime <- liftIO getCPUTime
    let t = fromIntegral (endTime - startTime) * 1e-9
    liftIO $ metricLogsPairTimeMs t
    liftIO metricLogsPairCount
    return a

newtype MyAwsM a = MyAwsM
    { unMyAwsM :: (ExceptT InterpreterError) (AWST' RuntimeEnvironment (ResourceT IO)) a
    } 

deriving instance Functor MyAwsM

deriving instance Applicative MyAwsM

deriving instance Monad MyAwsM

instance (MonadError InterpreterError) MyAwsM where
    throwError e = MyAwsM $ throwError e
    catchError m e = MyAwsM $ catchError (unMyAwsM m) (unMyAwsM . e)

instance (MonadReader RuntimeEnvironment) MyAwsM where
    ask = MyAwsM ask
    local r m = MyAwsM $ local r (unMyAwsM m)

instance MonadResource MyAwsM where
    liftResourceT = MyAwsM . liftResourceT

instance (MonadBase IO) MyAwsM where
    liftBase = MyAwsM . liftBase

instance MonadIO MyAwsM where
    liftIO = MyAwsM . liftIO

instance MonadThrow MyAwsM where
    throwM = MyAwsM . throwM

instance MonadCatch MyAwsM where
    catch m e = MyAwsM $ Control.Monad.Catch.catch (unMyAwsM m) (unMyAwsM . e)

allErrors
    :: (MonadError InterpreterError m)
    => [Either InterpreterError a] -> m [a]
allErrors l = 
    let loop (Left s:_) _ = throwError s
        loop [] acc = return acc
        loop (Right a:xs) acc = loop xs (a : acc)
    in reverse <$> loop l []

itemAttribute :: Text
              -> Lens' AttributeValue (Maybe v)
              -> v
              -> (Text, AttributeValue)
itemAttribute key l value = (key, set l (Just value) attributeValue)

writeToDynamoAws :: DynamoKey
                 -> DynamoValues
                 -> DynamoVersion
                 -> MyAwsM DynamoWriteResult
writeToDynamoAws DynamoKey{dynamoKeyKey = streamId,dynamoKeyEventNumber = eventNumber} values version = 
    catches
        writeItem
        [ handler
              _ConditionalCheckFailedException
              (\_ -> 
                    return DynamoWriteWrongVersion)]
  where
    addVersionChecks 0 req = 
        req &
        set
            piConditionExpression
            (Just $ "attribute_not_exists(" <> fieldEventNumber <> ")")
    addVersionChecks _ req = 
        req &
        set piConditionExpression (Just $ fieldVersion <> " = :itemVersion") &
        set
            piExpressionAttributeValues
            (HM.singleton
                 ":itemVersion"
                 (set avN (Just (showt (version - 1))) attributeValue))
    writeItem = do
        tn <- view runtimeEnvironmentTableName
        let item = 
                HM.fromList
                    [ itemAttribute fieldStreamId avS streamId
                    , itemAttribute fieldEventNumber avN (showt eventNumber)
                    , itemAttribute fieldVersion avN (showt version)] <>
                values
        let req0 = putItem tn & set piItem item & addVersionChecks version
        _ <- timeAction metricLogsWriteItem $ send req0
        return DynamoWriteSuccess

updateItemAws :: DynamoKey -> (HashMap Text ValueUpdate) -> MyAwsM Bool
updateItemAws DynamoKey{dynamoKeyKey = streamId,dynamoKeyEventNumber = eventNumber} values = 
    go
  where
    getSetExpressions :: Text -> ValueUpdate -> [Text] -> [Text]
    getSetExpressions _key ValueUpdateDelete xs = xs
    getSetExpressions key (ValueUpdateSet _value) xs = 
        let x = key <> "= :" <> key
        in x : xs
    getSetAttributeValues
        :: Text
        -> ValueUpdate
        -> HashMap Text AttributeValue
        -> HashMap Text AttributeValue
    getSetAttributeValues _key ValueUpdateDelete xs = xs
    getSetAttributeValues key (ValueUpdateSet value) xs = 
        HM.insert (":" <> key) value xs
    getRemoveExpressions x ValueUpdateDelete xs = x : xs
    getRemoveExpressions _key (ValueUpdateSet _value) xs = xs
    go = do
        tn <- view runtimeEnvironmentTableName
        let key = 
                HM.fromList
                    [ itemAttribute fieldStreamId avS streamId
                    , itemAttribute fieldEventNumber avN (showt eventNumber)]
        let setExpressions = HM.foldrWithKey getSetExpressions [] values
        let setExpression = 
                if null setExpressions
                    then []
                    else ["SET " ++ (T.intercalate ", " setExpressions)]
        let expressionAttributeValues = 
                HM.foldrWithKey getSetAttributeValues HM.empty values
        let removeKeys = HM.foldrWithKey getRemoveExpressions [] values
        let removeExpression = 
                if null removeKeys
                    then []
                    else ["REMOVE " ++ (T.intercalate ", " removeKeys)]
        let updateExpression = 
                T.intercalate " " (setExpression ++ removeExpression)
        let req0 = 
                Network.AWS.DynamoDB.updateItem tn & set uiKey key &
                set uiUpdateExpression (Just updateExpression) &
                if (not . HM.null) expressionAttributeValues
                    then set
                             uiExpressionAttributeValues
                             expressionAttributeValues
                    else id
        _ <- timeAction metricLogsUpdateItem $ send req0
        return True

queryTableAws :: QueryDirection
              -> Text
              -> Natural
              -> Maybe Int64
              -> MyAwsM [DynamoReadResult]
queryTableAws direction streamId limit exclusiveStartKey = getBackward
  where
    setStartKey Nothing = id
    setStartKey (Just startKey) = 
        set
            qExclusiveStartKey
            (HM.fromList
                 [ ("streamId", set avS (Just streamId) attributeValue)
                 , ( "eventNumber"
                   , set avN (Just $ show startKey) attributeValue)])
    scanForward = direction == QueryDirectionForward
    getBackward = do
        tn <- view runtimeEnvironmentTableName
        resp <- 
            timeAction metricLogsQuery $
            send $
            query tn & setStartKey exclusiveStartKey &
            set qConsistentRead (Just True) &
            set qLimit (Just limit) &
            set qScanIndexForward (Just scanForward) &
            set
                qExpressionAttributeValues
                (HM.fromList
                     [(":streamId", set avS (Just streamId) attributeValue)]) &
            set
                qKeyConditionExpression
                (Just $ fieldStreamId <> " = :streamId")
        let items :: [HM.HashMap Text AttributeValue] = view qrsItems resp
        let parsedItems = fmap toDynamoReadResult items
        allErrors parsedItems

newtype InMemoryCache k v =
    InMemoryCache (TVar (LRU.LRU k v))

newCacheAws
    :: (MonadIO m, Ord k)
    => Integer -> m (InMemoryCache k v)
newCacheAws size = do
    let cache = LRU.newLRU (Just size)
    tvarCache <- liftIO $ newTVarIO cache
    return $ InMemoryCache tvarCache

cacheLookupAws
    :: Ord k
    => InMemoryCache k v -> k -> MyAwsM (Maybe v)
cacheLookupAws (InMemoryCache cache) key = do
    liftIO $ atomically go
  where
    go = do
        lruCache <- readTVar cache
        let (lruCache',result) = LRU.lookup key lruCache
        writeTVar cache lruCache'
        return result

cacheInsertAws
    :: Ord k
    => InMemoryCache k v -> k -> v -> MyAwsM ()
cacheInsertAws (InMemoryCache cache) key value = 
    liftIO $ atomically $ modifyTVar cache go
  where
    go = LRU.insert key value

logAws :: LogLevel -> Text -> MyAwsM ()
logAws _logLeval msg = liftIO $ putStrLn msg

scanNeedsPagingAws :: MyAwsM [DynamoKey]
scanNeedsPagingAws = scanUnpaged
  where
    scanUnpaged = do
        tn <- view runtimeEnvironmentTableName
        resp <- 
            timeAction metricLogsScan $
            send $ scan tn & set sIndexName (Just unpagedIndexName)
        allErrors (fmap fromAttributesToDynamoKey (view srsItems resp))

waitAws :: Int -> MyAwsM ()
waitAws milliseconds = liftIO $ threadDelay (milliseconds * 1000)

setPulseStatusAws :: Bool -> MyAwsM ()
setPulseStatusAws _isActive = return ()

readFromDynamoAws :: DynamoKey -> MyAwsM (Maybe DynamoReadResult)
readFromDynamoAws eventKey = do
    tn <- view runtimeEnvironmentTableName
    let key = getDynamoKeyForEvent eventKey
    let req = getItem tn & set giKey key & set giConsistentRead (Just True)
    resp <- timeAction metricLogsReadItem $ send req
    result <- getResult resp
    return result
  where
    getResult
        :: (MonadError InterpreterError m)
        => GetItemResponse -> m (Maybe DynamoReadResult)
    getResult r = 
        let item = view girsItem r
        in if item == mempty
               then return Nothing
               else eitherToExcept (Just <$> toDynamoReadResult item)

readExcept
    :: (Read a)
    => (Text -> InterpreterError) -> Text -> Either InterpreterError a
readExcept err t = 
    let parsed = readMay t
    in case parsed of
           Nothing -> Left $ err t
           (Just a) -> Right a

toDynamoReadResult :: HM.HashMap Text AttributeValue
                   -> Either InterpreterError DynamoReadResult
toDynamoReadResult allValues = do
    let values = 
            allValues & HM.delete fieldVersion & HM.delete fieldStreamId &
            HM.delete fieldEventNumber
    eventKey <- fromAttributesToDynamoKey allValues
    version <- 
        readField fieldVersion avN allValues >>= readExcept EventVersionFormat
    return
        DynamoReadResult
        { dynamoReadResultKey = eventKey
        , dynamoReadResultVersion = version
        , dynamoReadResultValue = values
        }

readField
    :: (MonadError InterpreterError m)
    => Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField = readFieldGeneric FieldMissing

fromAttributesToDynamoKey :: HM.HashMap Text AttributeValue
                          -> Either InterpreterError DynamoKey
fromAttributesToDynamoKey allValues = do
    streamId <- readField fieldStreamId avS allValues
    eventNumber <- 
        readField fieldEventNumber avN allValues >>=
        readExcept EventNumberFormat
    return (DynamoKey streamId eventNumber)

readFieldGeneric
    :: (MonadError e m)
    => (Text -> e)
    -> Text
    -> Lens' AttributeValue (Maybe a)
    -> DynamoValues
    -> m a
readFieldGeneric toError fieldName fieldType values = 
    let fieldValue = values ^? ix fieldName
    in maybeToEither $ fieldValue >>= view fieldType
  where
    maybeToEither Nothing = throwError $ toError fieldName
    maybeToEither (Just x) = return x

buildTable :: Text -> MyAwsM ()
buildTable tableName = 
    MyAwsM $
    do let unpagedGlobalSecondary = 
               globalSecondaryIndex
                   unpagedIndexName
                   (keySchemaElement fieldPagingRequired Hash :| [])
                   (set pProjectionType (Just KeysOnly) projection)
                   (provisionedThroughput 1 1)
       let attributeDefinitions = 
               [ attributeDefinition fieldStreamId S
               , attributeDefinition fieldEventNumber N
               , attributeDefinition fieldPagingRequired S]
       let req0 = 
               createTable
                   tableName
                   (keySchemaElement fieldStreamId Hash :|
                    [keySchemaElement fieldEventNumber Range])
                   (provisionedThroughput 1 1) &
               set ctAttributeDefinitions attributeDefinitions &
               set ctGlobalSecondaryIndexes [unpagedGlobalSecondary]
       _ <- send req0
       _ <- 
           await
               (tableExists
                { _waitDelay = 4
                })
               (describeTable tableName)
       return ()

doesTableExist :: Text -> MyAwsM Bool
doesTableExist tableName = 
    catches
        describe
        [handler _ResourceNotFoundException (const $ return False)]
  where
    describe = do
        (resp :: DescribeTableResponse) <- send $ describeTable tableName
        let tableDesc = view drsTable resp
        return $ isJust tableDesc

evalProgram :: MetricLogs -> MyAwsM a -> IO (Either InterpreterError a)
evalProgram metrics program = do
    tableNameId :: Int <- getStdRandom (randomR (1, 9999999999))
    let tableName = "testtable-" ++ show tableNameId
    thisCompletePageQueue <- newTQueueIO
    awsEnv <- newEnv Sydney Discover
    let runtimeEnvironment = 
            RuntimeEnvironment
            { _runtimeEnvironmentMetricLogs = metrics
            , _runtimeEnvironmentCompletePageQueue = thisCompletePageQueue
            , _runtimeEnvironmentAmazonkaEnv = awsEnv
            , _runtimeEnvironmentTableName = tableName
            }
    _ <- runLocalDynamo runtimeEnvironment $ buildTable tableName
    runLocalDynamo runtimeEnvironment program

runLocalDynamo :: RuntimeEnvironment
               -> MyAwsM b
               -> IO (Either InterpreterError b)
runLocalDynamo runtimeEnvironment x = do
    let dynamo = setEndpoint False "localhost" 8000 dynamoDB
    env <- 
        newEnv
            Sydney
            (FromEnv "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" Nothing)
    let runtimeEnvironment' = 
            runtimeEnvironment
            { _runtimeEnvironmentAmazonkaEnv = env
            }
    runResourceT $
        runAWST runtimeEnvironment' $
        reconfigure dynamo $ runExceptT (unMyAwsM x)

type MyAwsStack = ((ExceptT InterpreterError) (AWST' RuntimeEnvironment (ResourceT IO)))

runProgram :: Text -> MyAwsM a -> MyAwsStack a
runProgram _tableName program = unMyAwsM program

runDynamoCloud2 :: RuntimeEnvironment
               -> AWST' RuntimeEnvironment (ResourceT IO) a
               -> IO a
runDynamoCloud2 runtimeEnvironment x = 
    runResourceT $ runAWST runtimeEnvironment $ x

runDynamoCloud :: RuntimeEnvironment
               -> MyAwsM a
               -> IO (Either InterpreterError a)
runDynamoCloud runtimeEnvironment x = 
    runResourceT $ runAWST runtimeEnvironment $ runExceptT $ unMyAwsM x
