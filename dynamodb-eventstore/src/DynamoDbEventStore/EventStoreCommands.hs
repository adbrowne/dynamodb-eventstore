{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE TypeFamilies               #-}

module DynamoDbEventStore.EventStoreCommands(
  StreamId(..),
  LogLevel(..),
  readField,
  readExcept,
  dynamoWriteWithRetry,
  updateItemWithRetry,
  MonadEsDsl(..),
  MonadEsDslWithFork(..),
  MyAwsM(..),
  DynamoVersion,
  QueryDirection(..),
  RecordedEvent(..),
  DynamoKey(..),
  DynamoWriteResult(..),
  DynamoReadResult(..),
  DynamoValues,
  ValueUpdate(..),
  PageKey(..),
  FeedEntry(..)
  ) where
import           BasicPrelude                              hiding (log)
import System.Metrics.Counter
import           Control.Concurrent.Async                  hiding (wait)
import           Control.Concurrent.STM
import           Control.Lens                              hiding ((.=))
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import qualified DodgerBlue.IO                             as DodgerIO
import           GHC.Natural

import           Control.Monad.Trans.AWS                   hiding (LogLevel)
import           Control.Monad.Trans.Resource
import           DynamoDbEventStore.AmazonkaImplementation
import           DynamoDbEventStore.Types
import           Network.AWS.DynamoDB                      hiding (updateItem)

class MonadEsDsl m => MonadEsDslWithFork m where
  forkChild :: Text -> m () -> m ()

class Monad m => MonadEsDsl m  where
  type QueueType m :: * -> *
  type CacheType m :: * -> * -> *
  type CounterType m :: *
  newCounter :: Text -> m (CounterType m)
  incrimentCounter :: CounterType m -> m ()
  newCache :: forall v k. (Typeable v, Typeable k, Ord k) => Integer -> m (CacheType m k v)
  cacheInsert :: (Typeable k, Ord k, Typeable v) => CacheType m k v -> k -> v -> m ()
  cacheLookup :: (Typeable k, Ord k, Typeable v) => CacheType m k v -> k -> m (Maybe v)
  newQueue :: forall a. Typeable a => m (QueueType m a)
  writeQueue :: forall a. Typeable a => QueueType m a -> a -> m ()
  readQueue :: forall a. Typeable a => QueueType m a -> m a
  tryReadQueue :: forall a. Typeable a => QueueType m a -> m (Maybe a)
  readFromDynamo :: DynamoKey -> m (Maybe DynamoReadResult)
  writeToDynamo :: DynamoKey -> DynamoValues -> DynamoVersion -> m DynamoWriteResult
  queryTable :: QueryDirection -> Text -> Natural -> Maybe Int64 -> m [DynamoReadResult]
  updateItem :: DynamoKey -> HashMap Text ValueUpdate -> m Bool
  log :: LogLevel -> Text -> m ()
  scanNeedsPaging :: m [DynamoKey]
  wait :: Int -> m ()
  setPulseStatus :: Bool -> m ()

forkChildIO :: Text -> MyAwsM () -> MyAwsM ()
forkChildIO _childThreadName (MyAwsM c) = MyAwsM $ do
  runtimeEnv <- ask
  _ <- lift $ allocate (async (runResourceT $ runAWST runtimeEnv (runExceptT c))) cancel
  return ()

instance MonadEsDsl MyAwsM where
  type QueueType MyAwsM = TQueue
  type CacheType MyAwsM = InMemoryCache
  type CounterType MyAwsM = Counter
  newQueue = MyAwsM DodgerIO.newQueue
  writeQueue q a = MyAwsM $ DodgerIO.writeQueue q a
  readQueue = MyAwsM . DodgerIO.readQueue
  tryReadQueue = MyAwsM . DodgerIO.tryReadQueue
  newCounter = newCounterAws
  incrimentCounter = incrimentCounterAws
  newCache = newCacheAws
  cacheInsert = cacheInsertAws
  cacheLookup = cacheLookupAws
  readFromDynamo = readFromDynamoAws
  writeToDynamo = writeToDynamoAws
  updateItem = updateItemAws
  queryTable = queryTableAws
  log = logAws
  scanNeedsPaging = scanNeedsPagingAws
  wait = waitAws
  setPulseStatus = setPulseStatusAws

instance MonadEsDslWithFork MyAwsM where
  forkChild = forkChildIO

instance MonadEsDsl m => MonadEsDsl (StateT s m) where
  type QueueType (StateT s m) = QueueType m
  type CacheType (StateT s m) = CacheType m
  type CounterType (StateT s m) = CounterType m
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
  newCounter = lift . newCounter
  incrimentCounter = lift . incrimentCounter
  newCache = lift . newCache
  cacheInsert a b c = lift $ cacheInsert a b c
  cacheLookup a b = lift $ cacheLookup a b
  readFromDynamo = lift . readFromDynamo
  writeToDynamo a b c = lift $ writeToDynamo a b c
  updateItem a b = lift $ updateItem a b
  queryTable a b c d = lift $ queryTable a b c d
  log a b = lift $ log a b
  scanNeedsPaging = lift scanNeedsPaging
  wait = lift . wait
  setPulseStatus = lift . setPulseStatus

instance MonadEsDsl m => MonadEsDsl (ExceptT e m) where
  type QueueType (ExceptT e m) = QueueType m
  type CacheType (ExceptT s m) = CacheType m
  type CounterType (ExceptT s m) = CounterType m
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
  newCounter = lift . newCounter
  incrimentCounter = lift . incrimentCounter
  newCache = lift . newCache
  cacheInsert a b c = lift $ cacheInsert a b c
  cacheLookup a b = lift $ cacheLookup a b
  readFromDynamo = lift . readFromDynamo
  writeToDynamo a b c = lift $ writeToDynamo a b c
  updateItem a b = lift $ updateItem a b
  queryTable a b c d = lift $ queryTable a b c d
  log a b = lift $ log a b
  scanNeedsPaging = lift scanNeedsPaging
  wait = lift . wait
  setPulseStatus = lift . setPulseStatus

readField :: (MonadError e m) => (Text -> e) -> Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField = readFieldGeneric

readExcept :: (MonadError e m) => (Read a) => (Text -> e) -> Text -> m a
readExcept err t =
  let
    parsed = BasicPrelude.readMay t
  in case parsed of Nothing  -> throwError $ err t
                    (Just a) -> return a

loopUntilSuccess :: Monad m => Integer -> (a -> Bool) -> m a -> m a
loopUntilSuccess maxTries f action =
  action >>= loop (maxTries - 1)
  where
    loop 0 lastResult = return lastResult
    loop _ lastResult | f lastResult = return lastResult
    loop triesRemaining _ = action >>= loop (triesRemaining - 1)

dynamoWriteWithRetry :: (MonadEsDsl m, MonadError EventStoreActionError m) => DynamoKey -> DynamoValues -> Int -> m DynamoWriteResult
dynamoWriteWithRetry key value version = do
  finalResult <- loopUntilSuccess 100 (/= DynamoWriteFailure) (writeToDynamo key value version)
  checkFinalResult finalResult
  where
    checkFinalResult DynamoWriteSuccess = return DynamoWriteSuccess
    checkFinalResult DynamoWriteWrongVersion = return DynamoWriteWrongVersion
    checkFinalResult DynamoWriteFailure = throwError $ EventStoreActionErrorWriteFailure key

updateItemWithRetry :: (MonadEsDsl m, MonadError EventStoreActionError m) => DynamoKey -> HashMap Text ValueUpdate -> m ()
updateItemWithRetry key updates = do
  result <- loopUntilSuccess 100 id (updateItem key updates)
  unless result (throwError $ EventStoreActionErrorUpdateFailure key)
