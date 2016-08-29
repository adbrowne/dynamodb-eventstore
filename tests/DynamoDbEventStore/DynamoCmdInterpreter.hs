{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE GADTs                      #-}

module DynamoDbEventStore.DynamoCmdInterpreter(
  TestState(..),
  runPrograms,
  runProgramGenerator,
  runProgram,
  emptyTestState,
  evalProgram,
  execProgram,
  execProgramUntilIdle,
  LoopState(..),
  loopStateDodgerState,
  testState,
  iopCounts,
  IopsCategory(..),
  IopsOperation(..)) where

import           BasicPrelude
import           DodgerBlue.Testing
import           Control.Lens
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Functor                           (($>))
import qualified Data.Map.Strict                        as Map
import qualified DynamoDbEventStore.InMemoryDynamoTable as MemDb
import           GHC.Natural
import qualified Prelude                                as P
import qualified Test.Tasty.QuickCheck                  as QC

import           DynamoDbEventStore.EventStoreCommands

newtype ProgramId = ProgramId { unProgramId :: Text } deriving (Eq, Show, Ord, IsString)

data LoopState r = LoopState {
  _loopStateTestState      :: TestState,
  _loopStateDodgerState      :: EvalState
}

data TestState = TestState {
  _testStateDynamo    :: MemDb.InMemoryDynamoTable,
  _testStateLog       :: Seq Text,
  _testStateIopCounts :: IopsTable
} -- deriving (Eq)

emptyTestState :: TestState
emptyTestState = TestState MemDb.emptyDynamoTable mempty mempty

data IopsCategory = UnpagedRead | UnpagedWrite | TableRead | TableWrite deriving (Eq, Show, Ord)

data IopsOperation = IopsScanUnpaged | IopsGetItem | IopsQuery | IopsWrite deriving (Eq, Show, Ord)

type IopsTable = Map (IopsCategory, IopsOperation, ProgramId) Int

$(makeFields ''LoopState)
$(makeLenses ''LoopState)
$(makeFields ''TestState)
$(makeLenses ''TestState)

addIops :: (MonadState s m, HasIopCounts s IopsTable, MonadReader ProgramId m) => IopsCategory -> IopsOperation -> Int -> m ()
addIops category operation i = do
  (programId :: ProgramId) <- ask
  iopCounts %= Map.insertWith (+) (category, operation, programId) i

instance HasIopCounts (LoopState a) IopsTable where
  iopCounts = loopStateTestState . testStateIopCounts

instance P.Show TestState where
  show a =
    "Dynamo: \n" <> P.show (_testStateDynamo a) <> "\n" <> "Log: \n" <> foldl' (\s l -> s <> "\n" <> P.show l) "" (_testStateLog a)

class Monad m => RandomFailure m where
  checkFail :: Double -> m Bool

instance RandomFailure Identity where
  checkFail _ = return False

instance RandomFailure QC.Gen where
  checkFail failurePercent = do
     r <- QC.choose (0, 99)
     return $ r < failurePercent

instance RandomFailure m => RandomFailure (StateT s m) where
   checkFail = lift . checkFail

instance RandomFailure m => RandomFailure (ReaderT r m) where
   checkFail = lift . checkFail

getGetReadResultCmd :: (MonadState TestState m, MonadReader ProgramId m) => DynamoKey -> (Maybe DynamoReadResult -> a) -> m a
getGetReadResultCmd key n = do
  addIops TableRead IopsGetItem 1
  db <- use (testStateDynamo)
  return $ n $ MemDb.readDb key db

addLog :: (MonadReader ProgramId m, MonadState TestState m) => Text -> m ()
addLog m = do
  (ProgramId programId) <- ask
  let appendMessage = flip (|>) (programId <> ": " <> m)
  testStateLog %= appendMessage

potentialFailure :: RandomFailure m => Double -> m a -> m a -> m a
potentialFailure failurePercent onFailure onSuccess = do
   didFail <- checkFail failurePercent
   if didFail
     then onFailure
     else onSuccess

runWriteToDynamoCmd :: (RandomFailure m, MonadState TestState m, MonadReader ProgramId m) => DynamoKey -> DynamoValues -> DynamoVersion -> (DynamoWriteResult -> n) -> m n
runWriteToDynamoCmd key values version next =
  potentialFailure 25 onFailure onSuccess
  where
    onFailure =
      addLog "Random write failure" $> next DynamoWriteFailure
    onSuccess = do
      addIops TableWrite IopsWrite 1
      (result, newDb) <- MemDb.writeDb key values version <$> use testStateDynamo
      case result of DynamoWriteWrongVersion -> addLog $ "Wrong version writing: " ++ show version
                     DynamoWriteSuccess -> addLog $ "Performing write: " ++ show key ++ " " ++ show values ++ " " ++ show version
                     DynamoWriteFailure -> addLog $ "Write failure: " ++ show key ++ " " ++ show values ++ " " ++ show version
      testStateDynamo .= newDb
      return $ next result

runQueryTableCmd :: (MonadState TestState m, MonadReader ProgramId m) => QueryDirection -> Text -> Natural -> Maybe Int64 -> ([DynamoReadResult] -> n) -> m n
runQueryTableCmd direction streamId maxEvents startEvent r =  do
  results <- uses testStateDynamo runQuery
  addIops TableRead IopsQuery $ length results
  return $ r results
  where
    runQuery = MemDb.queryDb direction streamId maxEvents startEvent

runUpdateItemCmd :: (MonadState TestState m, MonadReader ProgramId m, RandomFailure m) => DynamoKey -> HashMap Text ValueUpdate -> (Bool -> n) -> m n
runUpdateItemCmd key values next =
  potentialFailure 25 onFailure onSuccess
  where
    onFailure =
      addLog "Random updateItem failure" $> next False
    onSuccess = do
      addIops TableWrite IopsWrite 1
      newDb <- MemDb.updateDb key values <$> use testStateDynamo
      testStateDynamo .= newDb
      return $ next True

runScanNeedsPagingCmd :: (MonadState TestState m, MonadReader ProgramId m) => ([DynamoKey] -> n) -> m n
runScanNeedsPagingCmd n = do
   results <- uses testStateDynamo MemDb.scanNeedsPagingDb
   addIops UnpagedRead IopsScanUnpaged $ length results
   return $ n results

interpretDslCommand ::
  (MonadState TestState m, RandomFailure m) =>
  Text ->
  Text ->
  DynamoCmd a ->
  m a
interpretDslCommand _node threadName cmd =
  runReaderT (go cmd) (ProgramId threadName)
  where
    go :: (MonadState TestState m, RandomFailure m, MonadReader ProgramId m) => DynamoCmd a -> m a
    go (ReadFromDynamo' key n) =
      getGetReadResultCmd key n
    go (WriteToDynamo' key values version n) =
      runWriteToDynamoCmd key values version n
    go (QueryTable' direction key maxEvents startEvent n) =
      runQueryTableCmd direction key maxEvents startEvent n
    go (UpdateItem' key values n) = runUpdateItemCmd key values n
    go (ScanNeedsPaging' n) = runScanNeedsPagingCmd n
    go (Wait' _milliseconds n) = return n
    go (Log' _logLevel msg n) = (addLog msg >> return n)

runStateProgram :: ProgramId -> DynamoCmdM Queue a -> TestState -> (a,TestState)
runStateProgram (ProgramId programId) p initialTestState = runState (evalDslTest interpretDslCommand programId programId p) initialTestState

evalProgram :: ProgramId -> DynamoCmdM Queue a -> TestState -> a
evalProgram programId p initialTestState = fst $ runStateProgram programId p initialTestState

execProgram :: ProgramId -> DynamoCmdM Queue a -> TestState -> TestState
execProgram programId p initialTestState = snd $ runStateProgram programId p initialTestState

execProgramUntilIdle :: ProgramId -> DynamoCmdM Queue a -> TestState -> TestState
execProgramUntilIdle = execProgram

runPrograms' :: ExecutionTree (TestProgram DynamoCmd a) -> QC.Gen (ExecutionTree (ThreadResult a), TestState)
runPrograms' t =
  let
    interpretDslCommand' = interpretDslCommand :: Text -> Text -> DynamoCmd a -> StateT TestState QC.Gen a
  in runStateT (evalMultiDslTest interpretDslCommand' emptyEvalState t) emptyTestState

runPrograms :: Map ProgramId (DynamoCmdM Queue a, Int) -> QC.Gen (Map ProgramId a, TestState)
runPrograms programs =
  let
    startExecutionTree = ExecutionTree $ Map.singleton "node" $ (Map.mapKeys unProgramId $ fst <$> programs)
    mapResult (ExecutionTree t) = Map.foldr accOuter Map.empty t
  in do
    over _1 mapResult <$> (runPrograms' startExecutionTree)
  where
    accOuter :: Map Text (ThreadResult a) -> Map ProgramId a -> Map ProgramId a
    accOuter v z = Map.foldrWithKey accInner z v
    accInner :: Text -> ThreadResult a -> Map ProgramId a -> Map ProgramId a
    accInner threadName (ThreadResult a) m = Map.insert (ProgramId threadName) a m
    accInner _threadName ThreadBlocked m = m
    accInner _threadName ThreadIdle m = m

runProgramGenerator :: ProgramId -> DynamoCmdM Queue a -> TestState -> QC.Gen a
runProgramGenerator (ProgramId programId) p initialTestState = fst <$> runStateT (evalDslTest interpretDslCommand programId programId p) initialTestState 

runProgram :: ProgramId -> DynamoCmdM Queue a -> TestState -> (a, LoopState a)
runProgram = error "todo runProgram"
