{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}

module DynamoDbEventStore.EventStoreCommands(
  StreamId(..),
  LogLevel(..),
  log',
  writeToDynamo',
  readFromDynamo',
  wait',
  scanNeedsPaging',
  queryTable',
  updateItem',
  readField,
  Cache(..),
  MonadEsDsl(..),
  MonadEsDslWithFork(..),
  MyAwsM(..),
  DynamoVersion,
  QueryDirection(..),
  RecordedEvent(..),
  DynamoCmd(..),
  DynamoCmdM,
  DynamoKey(..),
  DynamoWriteResult(..),
  DynamoReadResult(..),
  DynamoValues,
  ValueUpdate(..),
  PageKey(..),
  FeedEntry(..)
  ) where
import           BasicPrelude                              hiding (log)
import           Control.Concurrent.Async                  hiding (wait)
import           Control.Concurrent.STM
import           Control.Lens                              hiding ((.=))
import           Control.Monad.Except
import           Control.Monad.Free.Church
import           Control.Monad.Reader
import           Control.Monad.State
import qualified DodgerBlue
import qualified DodgerBlue.IO                             as DodgerIO
import qualified DodgerBlue.Testing
import           GHC.Natural

import           Control.Monad.Trans.AWS                   hiding (LogLevel)
import           Control.Monad.Trans.Resource
import           DynamoDbEventStore.AmazonkaImplementation
import           DynamoDbEventStore.Types
import           Network.AWS.DynamoDB                      hiding (updateItem)

newtype Cache k v = Cache
    { unCache :: Int
    } deriving ((Show))

data DynamoCmd next where
  ReadFromDynamo' :: DynamoKey -> (Maybe DynamoReadResult -> next) -> DynamoCmd next
  WriteToDynamo' ::  DynamoKey -> DynamoValues -> DynamoVersion -> (DynamoWriteResult -> next) -> DynamoCmd next
  QueryTable' :: QueryDirection -> Text -> Natural -> Maybe Int64 -> ([DynamoReadResult] -> next) -> DynamoCmd next
  UpdateItem' :: DynamoKey -> HashMap Text ValueUpdate -> (Bool -> next) -> DynamoCmd next
  ScanNeedsPaging' :: ([DynamoKey] -> next) -> DynamoCmd next
  NewCache' :: (Typeable k, Typeable v) => Integer -> (Cache k v -> next) -> DynamoCmd next
  CacheInsert' :: (Ord k, Typeable k, Typeable v) => Cache k v -> k -> v -> next -> DynamoCmd next
  CacheLookup' :: (Typeable k, Ord k, Typeable v) => Cache k v -> k -> (Maybe v -> next) -> DynamoCmd next
  Wait' :: Int -> next -> DynamoCmd next
  Log' :: LogLevel -> Text -> next -> DynamoCmd next

deriving instance Functor DynamoCmd

class MonadEsDsl m => MonadEsDslWithFork m where
  forkChild :: m () -> m ()

class Monad m => MonadEsDsl m  where
  type QueueType m :: * -> *
  type CacheType m :: * -> * -> *
  newCache :: forall k. (Typeable k, Ord k) => forall v. Typeable v => Integer -> m (CacheType m k v)
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

wait' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => Int -> m ()
wait' waitSeconds = DodgerBlue.Testing.customCmd $ Wait' waitSeconds ()

log' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => LogLevel -> Text -> m ()
log' level message = DodgerBlue.Testing.customCmd $ Log' level message ()

scanNeedsPaging' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => m [DynamoKey]
scanNeedsPaging' = DodgerBlue.Testing.customCmd $ ScanNeedsPaging' id

updateItem' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> HashMap Text ValueUpdate -> m Bool
updateItem' key updates = DodgerBlue.Testing.customCmd $ UpdateItem' key updates id

readFromDynamo' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> m (Maybe DynamoReadResult)
readFromDynamo' key = DodgerBlue.Testing.customCmd $ ReadFromDynamo' key id

writeToDynamo' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> DynamoValues -> DynamoVersion -> m DynamoWriteResult
writeToDynamo' key values version = DodgerBlue.Testing.customCmd $ WriteToDynamo' key values version id

queryTable' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => QueryDirection -> Text -> Natural -> Maybe Int64 -> m [DynamoReadResult]
queryTable' direction hashKey maxEvents startEvent = DodgerBlue.Testing.customCmd $ QueryTable' direction hashKey maxEvents startEvent id

newCache' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m, Typeable k, Typeable v) => Integer -> m (Cache k v)
newCache' size = DodgerBlue.Testing.customCmd $ NewCache' size id

cacheInsert' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m, Ord k, Typeable k, Typeable v) => Cache k v -> k -> v -> m ()
cacheInsert' cache key value = DodgerBlue.Testing.customCmd $ CacheInsert' cache key value ()

cacheLookup' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m, Typeable k, Typeable v, Ord k) => Cache k v -> k -> m (Maybe v)
cacheLookup' cache key = DodgerBlue.Testing.customCmd $ CacheLookup' cache key id

type DynamoCmdM q = F (DodgerBlue.CustomDsl q DynamoCmd)

instance MonadEsDsl (F (DodgerBlue.CustomDsl q DynamoCmd)) where
  type QueueType (F (DodgerBlue.CustomDsl q DynamoCmd)) = q
  type CacheType (F (DodgerBlue.CustomDsl q DynamoCmd)) = Cache
  newQueue = DodgerBlue.newQueue
  newCache = newCache'
  cacheInsert = cacheInsert'
  cacheLookup = cacheLookup'
  writeQueue = DodgerBlue.writeQueue
  readQueue = DodgerBlue.readQueue
  tryReadQueue = DodgerBlue.tryReadQueue
  readFromDynamo = readFromDynamo'
  writeToDynamo = writeToDynamo'
  updateItem = updateItem'
  queryTable = queryTable'
  log = log'
  scanNeedsPaging = scanNeedsPaging'
  wait = wait'
  setPulseStatus = DodgerBlue.setPulseStatus

instance MonadEsDslWithFork (F (DodgerBlue.CustomDsl q DynamoCmd)) where
  forkChild = DodgerBlue.forkChild

forkChildIO :: MyAwsM () -> MyAwsM ()
forkChildIO (MyAwsM c) = MyAwsM $ do
  runtimeEnv <- ask
  _ <- lift $ allocate (async (runResourceT $ runAWST runtimeEnv (runExceptT c))) cancel
  return ()

instance MonadEsDsl MyAwsM where
  type QueueType MyAwsM = TQueue
  type CacheType MyAwsM = InMemoryCache
  newQueue = MyAwsM DodgerIO.newQueue
  writeQueue q a = MyAwsM $ DodgerIO.writeQueue q a
  readQueue = MyAwsM . DodgerIO.readQueue
  tryReadQueue = MyAwsM . DodgerIO.tryReadQueue
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
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
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
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
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
