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

module DynamoDbEventStore.DynamoCmdInterpreter(TestState(..), runPrograms, runProgramGenerator, runProgram, emptyTestState, evalProgram, execProgram, execProgramUntilIdle, LoopState(..), testState, iopCounts, IopsCategory(..), IopsOperation(..)) where

import           BasicPrelude
import           Control.Lens
import qualified Control.Monad.Free                     as Free
import qualified Control.Monad.Free.Church              as Church
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Functor                           (($>))
import qualified Data.HashMap.Lazy                      as HM
import qualified Data.Map.Strict                        as Map
import qualified Data.Text                              as T
import qualified DynamoDbEventStore.InMemoryDynamoTable as MemDb
import           GHC.Natural
import qualified Prelude                                as P
import qualified Test.Tasty.QuickCheck                  as QC

import qualified DynamoDbEventStore.Constants           as Constants
import           DynamoDbEventStore.EventStoreCommands

type DynamoCmdMFree = Free.Free DynamoCmd

--type TestDynamoTable = Map DynamoKey (Int, DynamoValues)

data RunningProgramState r = RunningProgramState {
  runningProgramStateNext    :: DynamoCmdMFree r,
  runningProgramStateMaxIdle :: Int
}

data LoopActivity =
  LoopActive { _loopActiveIdleCount :: Map ProgramId Bool } |
  LoopAllIdle { _loopAllIdleIterationsRemaining :: Map ProgramId Int }

data LoopState r = LoopState {
  _loopStateIterations     :: Int,
  _loopStateActivity       :: LoopActivity,
  _loopStateTestState      :: TestState,
  _loopStatePrograms       :: Map ProgramId (RunningProgramState r),
  _loopStateProgramResults :: Map ProgramId r
}

newtype ProgramId = ProgramId Text deriving (Eq, Show, Ord, IsString)

data IopsCategory = UnpagedRead | UnpagedWrite | TableRead | TableWrite deriving (Eq, Show, Ord)

data IopsOperation = IopsScanUnpaged | IopsGetItem | IopsQuery | IopsWrite deriving (Eq, Show, Ord)

type IopsTable = Map (IopsCategory, IopsOperation, ProgramId) Int

data TestState = TestState {
  _testStateDynamo    :: MemDb.InMemoryDynamoTable,
  _testStateLog       :: Seq Text,
  _testStateIopCounts :: IopsTable
} deriving (Eq)

instance P.Show TestState where
  show a =
    "Dynamo: \n" <> P.show (_testStateDynamo a) <> "\n" <> "Log: \n" <> foldl' (\s l -> s <> "\n" <> P.show l) "" (_testStateLog a)

$(makeFields ''TestState)
$(makeFields ''LoopState)
$(makeLenses ''TestState)
$(makeLenses ''LoopState)

instance HasIopCounts (LoopState a) IopsTable where
  iopCounts = loopStateTestState . testStateIopCounts

addIops :: (MonadState s m, HasIopCounts s IopsTable, MonadReader ProgramId m) => IopsCategory -> IopsOperation -> Int -> m ()
addIops category operation i = do
  (programId :: ProgramId) <- ask
  iopCounts %= Map.insertWith (+) (category, operation, programId) i

emptyTestState :: TestState
emptyTestState = TestState MemDb.emptyDynamoTable mempty mempty

type InterpreterApp m a r = RandomFailure m => (StateT (LoopState a) m) r

type InterpreterOperationStack m a r = RandomFailure m => ReaderT ProgramId (StateT (LoopState a) m) r

maxIterations :: Int
maxIterations = 100000

isComplete :: (MonadState (LoopState a) m) => m Bool
isComplete = do
  allProgramsComplete <- uses loopStatePrograms Map.null
  tooManyIterations <- uses loopStateIterations (> maxIterations)
  allProgramsOverMaxIdle <- uses loopStateActivity allOverMaxIdle
  return $ allProgramsComplete || tooManyIterations || allProgramsOverMaxIdle
  where
    allOverMaxIdle :: LoopActivity -> Bool
    allOverMaxIdle (LoopActive _) = False
    allOverMaxIdle (LoopAllIdle status) = Map.filter (>0) status == mempty

addLog :: Text -> InterpreterOperationStack m a ()
addLog m = do
  (ProgramId programId) <- ask
  let appendMessage = flip (|>) (programId <> ": " <> m)
  (loopStateTestState . testStateLog) %= appendMessage

class Monad m => RandomFailure m where
  checkFail :: Double -> m Bool

instance RandomFailure QC.Gen where
  checkFail failurePercent = do
     r <- QC.choose (0, 99)
     return $ r < failurePercent

instance RandomFailure Identity where
   checkFail _ = return False

instance (Monad m, RandomFailure m) => RandomFailure (ReaderT ProgramId (StateT (LoopState a) m)) where
   checkFail p = lift . lift $ checkFail p

potentialFailure :: Double -> InterpreterOperationStack m a r -> InterpreterOperationStack m a r -> InterpreterOperationStack m a r
potentialFailure failurePercent onFailure onSuccess = do
   didFail <- checkFail failurePercent
   if didFail
     then onFailure
     else onSuccess

writeToDynamo :: DynamoKey -> DynamoValues -> DynamoVersion -> (DynamoWriteResult -> n) -> InterpreterOperationStack m a n
writeToDynamo key values version next =
  potentialFailure 25 onFailure onSuccess
  where
    onFailure =
      addLog "Random write failure" $> next DynamoWriteFailure
    onSuccess = do
      addIops TableWrite IopsWrite 1
      (result, newDb) <- MemDb.writeDb key values version <$> use (loopStateTestState . testStateDynamo)
      case result of DynamoWriteWrongVersion -> addLog $ "Wrong version writing: " ++ show version
                     DynamoWriteSuccess -> addLog $ "Performing write: " ++ show key ++ " " ++ show values ++ " " ++ show version
                     DynamoWriteFailure -> addLog $ "Write failure: " ++ show key ++ " " ++ show values ++ " " ++ show version
      (loopStateTestState . testStateDynamo) .= newDb
      return $ next result

getReadResult :: DynamoKey -> (Maybe DynamoReadResult -> n) -> InterpreterOperationStack m a n
getReadResult key n = do
  addIops TableRead IopsGetItem 1
  db <- use (testState . testStateDynamo)
  return $ n $ MemDb.readDb key db

queryTable :: QueryDirection -> Text -> Natural -> Maybe Int64 -> ([DynamoReadResult] -> n) -> InterpreterOperationStack m a n
queryTable direction streamId maxEvents startEvent r =  do
  results <- uses (loopStateTestState . testStateDynamo) runQuery
  addIops TableRead IopsQuery $ length results
  return $ r results
  where
    runQuery = MemDb.queryDb direction streamId maxEvents startEvent

scanNeedsPaging :: ([DynamoKey] -> n) -> InterpreterOperationStack m a n
scanNeedsPaging n = do
   results <- uses (testState . testStateDynamo) MemDb.scanNeedsPagingDb
   addIops UnpagedRead IopsScanUnpaged $ length results
   return $ n results

setPulseStatus :: Bool -> InterpreterOperationStack m a ()
setPulseStatus isActive = do
  programId <- ask
  allInactiveState <- uses loopStatePrograms initialAllInactive
  loopStateActivity %= updatePulseStatus programId (LoopAllIdle allInactiveState) isActive
  where
    allInactive :: Map ProgramId Bool -> Bool
    allInactive m = Map.filter id m == mempty
    initialAllInactive :: Map ProgramId (RunningProgramState a) -> Map ProgramId Int
    initialAllInactive = fmap runningProgramStateMaxIdle
    updatePulseStatus :: ProgramId -> LoopActivity -> Bool -> LoopActivity -> LoopActivity
    updatePulseStatus programId allInactiveState _ LoopActive { _loopActiveIdleCount = idleCount } =
      let updated = Map.alter (const (Just isActive)) programId idleCount
      in if allInactive updated then allInactiveState else LoopActive updated
    updatePulseStatus _ _ True LoopAllIdle { _loopAllIdleIterationsRemaining = _idleRemaining } = LoopActive mempty
    updatePulseStatus programId _ False LoopAllIdle { _loopAllIdleIterationsRemaining = idleRemaining } = LoopAllIdle $ Map.adjust (\x -> x - 1) programId idleRemaining

runCmd :: DynamoCmdMFree r -> InterpreterOperationStack m r (Either r (DynamoCmdMFree r))
runCmd (Free.Pure r) = return $ Left r
runCmd (Free.Free (Wait' _ r)) = Right <$> return r
runCmd (Free.Free (QueryTable' direction key maxEvents start r)) = Right <$> queryTable direction key maxEvents start r
runCmd (Free.Free (WriteToDynamo' key values version r)) = Right <$> writeToDynamo key values version r
runCmd (Free.Free (ReadFromDynamo' key r)) = Right <$> getReadResult key r
runCmd (Free.Free (Log' _ msg r)) = Right <$> (addLog msg >> return r)
runCmd (Free.Free (SetPulseStatus' isActive r)) = Right <$> (setPulseStatus isActive >> return r)
runCmd (Free.Free (ScanNeedsPaging' r)) = Right <$> scanNeedsPaging r

stepProgram :: ProgramId -> RunningProgramState r -> InterpreterApp m r ()
stepProgram programId ps = do
  cmdResult <- runReaderT (runCmd $ runningProgramStateNext ps) programId
  bigBanana cmdResult
  where
    bigBanana :: Either r (DynamoCmdMFree r) -> InterpreterApp m r ()
    bigBanana next = do
      let result = fmap setNextProgram next
      updateLoopState programId result
    setNextProgram :: DynamoCmdMFree r -> RunningProgramState r
    setNextProgram n = ps { runningProgramStateNext = n }

updateLoopState :: ProgramId -> Either r (RunningProgramState r) -> InterpreterApp m r ()
updateLoopState programName result =
  loopStatePrograms %= updateProgramEntry result >>
  loopStateProgramResults %= updateProgramResults result
  where
    updateProgramEntry (Left _)         = Map.delete programName
    updateProgramEntry (Right newState) = Map.adjust (const newState) programName
    updateProgramResults (Left r)       = Map.insert programName r
    updateProgramResults (Right _)        = id

iterateApp :: InterpreterApp QC.Gen a ()
iterateApp = do
 p <- use loopStatePrograms
 (programId, programState) <- lift . QC.elements $ Map.toList p
 stepProgram programId programState

runPrograms :: Map ProgramId (DynamoCmdM a, Int) -> QC.Gen (Map ProgramId a, TestState)
runPrograms ps =
  over _2 (view loopStateTestState) <$> runStateT loop initialState
  where
        runningPrograms = fmap (\(p,maxIdleIterations) -> RunningProgramState (Church.fromF p) maxIdleIterations) ps
        initialState = LoopState 0 (LoopActive mempty) emptyTestState runningPrograms mempty
        loop :: InterpreterApp QC.Gen a (Map ProgramId a)
        loop = do
          complete <- isComplete
          if complete
             then use loopStateProgramResults
             else iterateApp >> loopStateIterations += 1 >> loop

runProgramGenerator :: ProgramId -> DynamoCmdM a -> TestState -> QC.Gen a
runProgramGenerator programId program initialTestState =
  evalStateT (runReaderT (loop $ Right (Church.fromF program)) programId) initialState
  where
        runningPrograms = Map.singleton programId (RunningProgramState (Church.fromF program) 0)
        initialState = LoopState 0 (LoopActive mempty) initialTestState runningPrograms mempty
        loop :: Either r (DynamoCmdMFree r) -> InterpreterOperationStack QC.Gen r r
        loop (Left x) = return x
        loop (Right n) = runCmd n >>= loop

runProgram :: ProgramId -> DynamoCmdM a -> TestState -> (a, LoopState a)
runProgram programId program initialTestState =
  runIdentity $ runStateT (runReaderT (loop $ Right (Church.fromF program)) programId) initialState
  where
        runningPrograms = Map.singleton programId (RunningProgramState (Church.fromF program) 0)
        initialState = LoopState 0 (LoopActive mempty) initialTestState runningPrograms mempty
        loop :: Either r (DynamoCmdMFree r) -> InterpreterOperationStack Identity r r
        loop (Left x) = return x
        loop (Right n) = runCmd n >>= loop

execProgramUntilIdle :: ProgramId -> DynamoCmdM a -> TestState -> LoopState a
execProgramUntilIdle programId program initialTestState =
  runIdentity $ execStateT (runReaderT (loop $ Right (Church.fromF program)) programId) initialState
  where
        runningPrograms = Map.singleton programId (RunningProgramState (Church.fromF program) 0)
        initialState = LoopState 0 (LoopActive mempty) initialTestState runningPrograms mempty
        loop :: Either r (DynamoCmdMFree r) -> InterpreterOperationStack Identity r (Maybe r)
        loop (Left x) = return $ Just x
        loop (Right n) = do
          complete <- isComplete
          if complete
              then return Nothing
              else runCmd n >>= loop

evalProgram :: ProgramId -> DynamoCmdM a -> TestState -> a
evalProgram programId program initialTestState = fst $ runProgram programId program initialTestState
execProgram :: ProgramId -> DynamoCmdM a -> TestState -> LoopState a
execProgram programId program initialTestState = snd $ runProgram programId program initialTestState
