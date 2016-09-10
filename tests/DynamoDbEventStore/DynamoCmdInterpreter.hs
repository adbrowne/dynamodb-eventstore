{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module DynamoDbEventStore.DynamoCmdInterpreter
  ( TestState(..)
  , LogEvent(..)
  , ProgramId(..)
  , runPrograms
  , runProgramsWithState
  , runProgramGenerator
  , emptyTestState
  , evalProgram
  , execProgram
  , execProgramUntilIdle
  , LoopState(..)
  , testState
  , testStateLog
  , IopsCategory(..)
  , IopsOperation(..)
  ) where

import           BasicPrelude
import qualified Debug.Trace
import           Control.Lens
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Functor                           (($>))
import qualified Data.Map.Strict                        as Map
import           DodgerBlue.Testing
import qualified DynamoDbEventStore.InMemoryCache       as MemCache
import qualified DynamoDbEventStore.InMemoryDynamoTable as MemDb
import           GHC.Natural
import qualified Data.Text as T
import qualified Prelude                                as P
import qualified Test.Tasty.QuickCheck                  as QC

import           DynamoDbEventStore.EventStoreCommands

newtype ProgramId = ProgramId
  { unProgramId :: Text
  } deriving (Eq, Show, Ord, IsString)

data LoopState r = LoopState
  { _loopStateTestState   :: TestState
  , _loopStateDodgerState :: EvalState
  }

data LogEvent =
  LogEventText LogLevel Text
  | LogEventIops IopsCategory IopsOperation ProgramId Int
  deriving (Show)

data TestState = TestState
  { _testStateDynamo :: MemDb.InMemoryDynamoTable
  , _testStateCache  :: MemCache.Caches
  , _testStateLog    :: Seq LogEvent
  }

emptyTestState :: TestState
emptyTestState = TestState MemDb.emptyDynamoTable MemCache.emptyCache mempty

data IopsCategory
  = UnpagedRead
  | UnpagedWrite
  | TableRead
  | TableWrite
  deriving (Eq, Show, Ord)

data IopsOperation
  = IopsScanUnpaged
  | IopsGetItem
  | IopsQuery
  | IopsWrite
  deriving (Eq, Show, Ord)

$(makeFields ''LoopState)

$(makeFields ''TestState)

$(makeLenses ''TestState)

addIops
  :: (MonadState TestState m, MonadReader ProgramId m)
  => IopsCategory -> IopsOperation -> Int -> m ()
addIops category operation i = do
  (programId :: ProgramId) <- ask
  addLogEvent $ LogEventIops category operation programId i

instance P.Show TestState where
  show a =
    "Dynamo: \n" <> P.show (_testStateDynamo a) <> "\n" <> "Log: \n" <>
    foldl' (\s l -> s <> "\n" <> P.show l) "" (_testStateLog a)

class Monad m =>
      RandomFailure m  where
  checkFail :: Double -> m Bool

instance RandomFailure Identity where
  checkFail _ = return False

instance RandomFailure QC.Gen where
  checkFail failurePercent = do
    r <- QC.choose (0, 99)
    return $ r < failurePercent

instance RandomFailure m =>
         RandomFailure (StateT s m) where
  checkFail = lift . checkFail

instance RandomFailure m =>
         RandomFailure (ReaderT r m) where
  checkFail = lift . checkFail

getGetReadResultCmd
  :: (MonadState TestState m, MonadReader ProgramId m)
  => DynamoKey -> (Maybe DynamoReadResult -> a) -> m a
getGetReadResultCmd key n = do
  addIops TableRead IopsGetItem 1
  db <- use (testStateDynamo)
  return $ n $ MemDb.readDb key db

addLogEvent
  :: (MonadReader ProgramId m, MonadState TestState m)
  => LogEvent -> m ()
addLogEvent evt = do
  let appendMessage = flip (|>) evt
  testStateLog %= appendMessage

addTextLog
  :: (MonadReader ProgramId m, MonadState TestState m)
  => LogLevel -> Text -> m ()
addTextLog level m = do
  (ProgramId programId) <- ask
  addLogEvent $ LogEventText level (programId <> ": " <> m)

potentialFailure
  :: RandomFailure m
  => Double -> m a -> m a -> m a
potentialFailure failurePercent onFailure onSuccess = do
  didFail <- checkFail failurePercent
  if didFail
    then onFailure
    else onSuccess

runWriteToDynamoCmd
  :: (RandomFailure m, MonadState TestState m, MonadReader ProgramId m)
  => DynamoKey
  -> DynamoValues
  -> DynamoVersion
  -> (DynamoWriteResult -> n)
  -> m n
runWriteToDynamoCmd key values version next = potentialFailure 25 onFailure onSuccess
  where
    onFailure = addTextLog Debug "Random write failure" $> next DynamoWriteFailure
    onSuccess = do
      addIops TableWrite IopsWrite 1
      (result, newDb) <- MemDb.writeDb key values version <$> use testStateDynamo
      case result of
        DynamoWriteWrongVersion -> addTextLog Debug $ "Wrong version writing: " ++ show version
        DynamoWriteSuccess ->
          addTextLog Debug $
          "Performing write: " ++
          show key ++ " " ++ show values ++ " " ++ show version
        DynamoWriteFailure ->
          addTextLog Debug $
          "Write failure: " ++ show key ++ " " ++ show values ++ " " ++ show version
      testStateDynamo .= newDb
      return $ next result

runQueryTableCmd
  :: (MonadState TestState m, MonadReader ProgramId m)
  => QueryDirection
  -> Text
  -> Natural
  -> Maybe Int64
  -> ([DynamoReadResult] -> n)
  -> m n
runQueryTableCmd direction streamId maxEvents startEvent r = do
  results <- uses testStateDynamo runQuery
  addIops TableRead IopsQuery $ length results
  return $ r results
  where
    runQuery = MemDb.queryDb direction streamId maxEvents startEvent

runUpdateItemCmd
  :: (MonadState TestState m, MonadReader ProgramId m, RandomFailure m)
  => DynamoKey -> HashMap Text ValueUpdate -> (Bool -> n) -> m n
runUpdateItemCmd key values next = potentialFailure 25 onFailure onSuccess
  where
    onFailure = addTextLog Debug "Random updateItem failure" $> next False
    onSuccess = do
      addIops TableWrite IopsWrite 1
      newDb <- MemDb.updateDb key values <$> use testStateDynamo
      testStateDynamo .= newDb
      return $ next True

runScanNeedsPagingCmd
  :: (MonadState TestState m, MonadReader ProgramId m)
  => ([DynamoKey] -> n) -> m n
runScanNeedsPagingCmd n = do
  results <- uses testStateDynamo MemDb.scanNeedsPagingDb
  addIops UnpagedRead IopsScanUnpaged $ length results
  return $ n results

runNewCacheCmd
  :: (MonadState TestState m)
  => Integer -> ((Cache k v) -> n) -> m n
runNewCacheCmd size n = do
  (cache', caches') <- uses testStateCache (MemCache.newCache size)
  testStateCache .= caches'
  return $ n cache'

runInsertCacheCmd
  :: (MonadState TestState m, Ord k, Typeable k, Typeable v)
  => Cache k v -> k -> v -> n -> m n
runInsertCacheCmd c k v n = do
  caches' <- uses testStateCache (MemCache.insertCache c k v)
  testStateCache .= caches'
  return n

runLookupCacheCmd
  :: (MonadState TestState m, Ord k, Typeable k, Typeable v)
  => Cache k v -> k -> (Maybe v -> n) -> m n
runLookupCacheCmd c k n = do
  (result, caches') <- uses testStateCache (MemCache.lookupCache c k)
  testStateCache .= caches'
  return $ n result

interpretDslCommand
  :: (MonadState TestState m, RandomFailure m)
  => Text -> DynamoCmd a -> m a
interpretDslCommand threadName cmd =
  runReaderT (go cmd) (ProgramId threadName)
  where
    go
      :: (MonadState TestState m, RandomFailure m, MonadReader ProgramId m)
      => DynamoCmd a -> m a
    go (ReadFromDynamo' key n) = getGetReadResultCmd key n
    go (WriteToDynamo' key values version n) =
      runWriteToDynamoCmd key values version n
    go (QueryTable' direction key maxEvents startEvent n) =
      runQueryTableCmd direction key maxEvents startEvent n
    go (UpdateItem' key values n) = runUpdateItemCmd key values n
    go (ScanNeedsPaging' n) = runScanNeedsPagingCmd n
    go (NewCache' size n) = runNewCacheCmd size n
    go (CacheInsert' c k v n) = runInsertCacheCmd c k v n
    go (CacheLookup' c k n) = runLookupCacheCmd c k n
    go (Wait' _milliseconds n) = return n
    go (Log' _logLevel msg n) = (addTextLog Debug msg >> return n)

myInterpret :: Text -> DynamoCmd a -> StateT TestState Identity a
myInterpret = interpretDslCommand

runStateProgram :: ProgramId
                -> DynamoCmdM Queue a
                -> TestState
                -> (a, TestState)
runStateProgram (ProgramId programId) p initialTestState =
  runState (evalDslTest myInterpret programId p) initialTestState

evalProgram :: ProgramId -> DynamoCmdM Queue a -> TestState -> a
evalProgram programId p initialTestState = fst $ runStateProgram programId p initialTestState

execProgram :: ProgramId -> DynamoCmdM Queue a -> TestState -> TestState
execProgram programId p initialTestState = snd $ runStateProgram programId p initialTestState

execProgramUntilIdle :: ProgramId -> DynamoCmdM Queue a -> TestState -> TestState
execProgramUntilIdle = execProgram

reportActiveThreads :: Monad m => Int -> Bool -> [Text] -> [Text] -> m ()
reportActiveThreads _count _inCooldown _active _idle = return ()

runPrograms'
  :: ExecutionTree (TestProgram DynamoCmd a)
  -> TestState
  -> QC.Gen (ExecutionTree (ThreadResult a), TestState)
runPrograms' t startTestState =
  let interpretDslCommand' =
        interpretDslCommand :: Text -> DynamoCmd a -> StateT TestState QC.Gen a
  in runStateT (evalMultiDslTest interpretDslCommand' reportActiveThreads emptyEvalState t) startTestState

runProgramsWithState :: Map ProgramId (DynamoCmdM Queue a, Int)
            -> TestState
            -> QC.Gen (Map ProgramId a, TestState)
runProgramsWithState programs startTestState =
  let startExecutionTree =
        ExecutionTree $ (Map.mapKeys unProgramId $ fst <$> programs)
      mapResult (ExecutionTree t) = Map.foldrWithKey accInner Map.empty t
  in do over _1 mapResult <$> (runPrograms' startExecutionTree startTestState)
  where
    accInner :: Text -> ThreadResult a -> Map ProgramId a -> Map ProgramId a
    accInner threadName (ThreadResult a) m =
      Map.insert (ProgramId threadName) a m
    accInner _threadName ThreadBlocked m = m
    accInner _threadName ThreadIdle m = m

runPrograms :: Map ProgramId (DynamoCmdM Queue a, Int)
            -> QC.Gen (Map ProgramId a, TestState)
runPrograms programs = runProgramsWithState programs emptyTestState

runProgramGenerator :: ProgramId -> DynamoCmdM Queue a -> TestState -> QC.Gen a
runProgramGenerator (ProgramId programId) p initialTestState =
  fst <$> runStateT (evalDslTest interpretDslCommand programId p) initialTestState
