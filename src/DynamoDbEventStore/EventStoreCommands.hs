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
  readExcept,
  dynamoWriteWithRetry,
  updateItemWithRetry,
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
import qualified Prelude
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

showResult :: String -> String -> String
showResult myString existing = myString <> existing

instance Show (DynamoCmd n) where
  showsPrec _ (ReadFromDynamo' key _) =
    showResult $ "ReadFromDynamo' " <> Prelude.show key
  showsPrec _ (WriteToDynamo' key values version _) =
    showResult $ "WriteToDynamo' " <> Prelude.show key <> " " <> Prelude.show values <> " " <> Prelude.show version
  showsPrec _ (QueryTable' direction key maxEvents startEvent _) =
    showResult $ "QueryTable' " <> Prelude.show direction <> " " <> Prelude.show key <> " " <> Prelude.show maxEvents <> " " <> Prelude.show startEvent
  showsPrec _ (UpdateItem' key _values _) = 
    showResult $ "UpdateItem' " <> Prelude.show key
  showsPrec _ (ScanNeedsPaging' _) =
    showResult "ScanNeedsPaging'"
  showsPrec _ (NewCache' size _) =
    showResult $ "NewCache' " <> Prelude.show size
  showsPrec _ (CacheInsert' c _ _ _) =
    showResult $ "CacheInsert' " <> Prelude.show c
  showsPrec _ (CacheLookup' c _ _) =
    showResult $ "CacheLookup' " <> Prelude.show c
  showsPrec _ (Wait' milliseconds _) =
    showResult $ "Wait' " <> Prelude.show milliseconds
  showsPrec _ (Log' logLevel msg _) =
    showResult $ "Log' " <> Prelude.show logLevel <> " " <> Prelude.show msg

deriving instance Functor DynamoCmd

class MonadEsDsl m => MonadEsDslWithFork m where
  forkChild :: Text -> m () -> m ()

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

forkChildIO :: Text -> MyAwsM () -> MyAwsM ()
forkChildIO childThreadName (MyAwsM c) = MyAwsM $ do
  runtimeEnv <- ask
  _ <- lift $ allocate (async (runResourceT $ runAWST runtimeEnv (runExceptT c))) onDispose
  return ()
  where
    onDispose a = do
      putStrLn ("disposing" <> childThreadName)
      cancel a

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
