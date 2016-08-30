{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

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
import           BasicPrelude hiding (log)
import           Control.Lens              hiding ((.=))
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Concurrent.STM
import           Control.Concurrent.Async hiding (wait)
import           Control.Monad.Free.Church
import qualified DodgerBlue
import qualified DodgerBlue.Testing
import qualified DodgerBlue.IO             as DodgerIO
import           GHC.Natural

import           DynamoDbEventStore.Types
import           DynamoDbEventStore.AmazonkaImplementation
import           Network.AWS.DynamoDB hiding (updateItem)
import           Control.Monad.Trans.AWS hiding (LogLevel)
import           Control.Monad.Trans.Resource


data DynamoCmd next =
  ReadFromDynamo' DynamoKey (Maybe DynamoReadResult -> next)
  | WriteToDynamo' DynamoKey DynamoValues DynamoVersion (DynamoWriteResult -> next)
  | QueryTable' QueryDirection Text Natural (Maybe Int64) ([DynamoReadResult] -> next)
  | UpdateItem' DynamoKey (HashMap Text ValueUpdate) (Bool -> next)
  | ScanNeedsPaging' ([DynamoKey] -> next)
  | Wait' Int next
  | Log' LogLevel Text next

deriving instance Functor DynamoCmd

class MonadEsDsl m => MonadEsDslWithFork m where
  forkChild :: m () -> m ()
  
class Monad m => MonadEsDsl m  where
  type QueueType m :: * -> *
  newQueue :: forall a. Typeable a => m (QueueType m a)
  writeQueue :: forall a. Typeable a => QueueType m a -> a -> m ()
  readQueue :: forall a. Typeable a => QueueType m a -> m a
  tryReadQueue :: forall a. Typeable a => QueueType m a -> m (Maybe a)
  readFromDynamo :: DynamoKey -> m (Maybe DynamoReadResult)
  writeToDynamo :: DynamoKey -> DynamoValues -> DynamoVersion -> m (DynamoWriteResult)
  queryTable :: QueryDirection -> Text -> Natural -> Maybe Int64 -> m [DynamoReadResult]
  updateItem :: DynamoKey -> (HashMap Text ValueUpdate) -> m Bool
  log :: LogLevel -> Text -> m ()
  scanNeedsPaging :: m [DynamoKey]
  wait :: Int -> m ()
  setPulseStatus :: Bool -> m ()

wait' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => Int -> m ()
wait' waitSeconds = DodgerBlue.Testing.customCmd $ Wait' waitSeconds ()

log' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => LogLevel -> Text -> m ()
log' level message = DodgerBlue.Testing.customCmd $ Log' level message ()

scanNeedsPaging' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => m ([DynamoKey])
scanNeedsPaging' = DodgerBlue.Testing.customCmd $ ScanNeedsPaging' id

updateItem' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> (HashMap Text ValueUpdate) -> m (Bool)
updateItem' key updates = DodgerBlue.Testing.customCmd $ UpdateItem' key updates id

readFromDynamo' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> m (Maybe DynamoReadResult)
readFromDynamo' key = DodgerBlue.Testing.customCmd $ ReadFromDynamo' key id

writeToDynamo' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> DynamoValues -> DynamoVersion -> m (DynamoWriteResult)
writeToDynamo' key values version = DodgerBlue.Testing.customCmd $ WriteToDynamo' key values version id

queryTable' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => QueryDirection -> Text -> Natural -> Maybe Int64 -> m ([DynamoReadResult])
queryTable' direction hashKey maxEvents startEvent = DodgerBlue.Testing.customCmd $ QueryTable' direction hashKey maxEvents startEvent id

type DynamoCmdM q = F (DodgerBlue.CustomDsl q DynamoCmd)

instance MonadEsDsl (F (DodgerBlue.CustomDsl q DynamoCmd)) where
  type QueueType (F (DodgerBlue.CustomDsl q DynamoCmd)) = q
  newQueue = DodgerBlue.newQueue
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

--type MyAwsStack = ((ExceptT InterpreterError) (AWST' RuntimeEnvironment (ResourceT IO)))

forkChildIO :: MyAwsM () -> MyAwsM ()
forkChildIO (MyAwsM c) = MyAwsM $ do
  runtimeEnv <- ask
  _ <- lift $ allocate (async (runResourceT $ runAWST runtimeEnv (runExceptT c))) cancel
  return ()

instance MonadEsDsl MyAwsM where
  type QueueType MyAwsM = TQueue
  newQueue = MyAwsM $ DodgerIO.newQueue
  writeQueue q a = MyAwsM $ DodgerIO.writeQueue q a
  readQueue = MyAwsM . DodgerIO.readQueue
  tryReadQueue = MyAwsM . DodgerIO.tryReadQueue
  readFromDynamo = readFromDynamoAws
  writeToDynamo = writeToDynamoAws
  updateItem = updateItemAws
  queryTable = queryTableAws
  log = logAws
  scanNeedsPaging = scanNeedsPagingAws
  wait = waitAws
  setPulseStatus = setPulseStatusAws

instance MonadEsDslWithFork MyAwsM where
  forkChild c = forkChildIO c

instance MonadEsDsl m => MonadEsDsl (StateT s m) where
  type QueueType (StateT s m) = QueueType m
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
  readFromDynamo = lift . readFromDynamo
  writeToDynamo a b c = lift $ writeToDynamo a b c
  updateItem a b = lift $ updateItem a b
  queryTable a b c d = lift $ queryTable a b c d
  log a b = lift $ log a b
  scanNeedsPaging = lift $ scanNeedsPaging
  wait = lift . wait
  setPulseStatus = lift . setPulseStatus

instance MonadEsDsl m => MonadEsDsl (ExceptT e m) where
  type QueueType (ExceptT e m) = QueueType m
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
  readFromDynamo = lift . readFromDynamo
  writeToDynamo a b c = lift $ writeToDynamo a b c
  updateItem a b = lift $ updateItem a b
  queryTable a b c d = lift $ queryTable a b c d
  log a b = lift $ log a b
  scanNeedsPaging = lift $ scanNeedsPaging
  wait = lift . wait
  setPulseStatus = lift . setPulseStatus

readField :: (MonadError e m) => (Text -> e) -> Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField = readFieldGeneric
