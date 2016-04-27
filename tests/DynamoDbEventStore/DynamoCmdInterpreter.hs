{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module DynamoDbEventStore.DynamoCmdInterpreter(ProgramError(FatalError), TestState(..), runPrograms, runProgramGenerator, runProgram, emptyTestState, evalProgram, execProgram, LoopState(..)) where

import           BasicPrelude
import           GHC.Natural
import qualified Prelude as P
import           Control.Lens
import           Control.Monad.State
import           Data.Functor (($>))
import qualified Data.HashMap.Lazy as HM
import qualified Control.Monad.Free.Church as Church
import qualified Test.Tasty.QuickCheck as QC
import qualified Control.Monad.Free as Free
import qualified Data.Map.Strict as Map

import qualified DynamoDbEventStore.Constants as Constants
import           DynamoDbEventStore.EventStoreCommands

type DynamoCmdMFree = Free.Free DynamoCmd

type TestDynamoTable = Map DynamoKey (Int, DynamoValues)

data RunningProgramState r = RunningProgramState {
  runningProgramStateNext :: DynamoCmdMFree r,
  runningProgramStateMaxIdle :: Int
}

data LoopActivity = 
  LoopActive { _loopActiveIdleCount :: Map Text Bool } |
  LoopAllIdle { _loopAllIdleIterationsRemaining :: Map Text Int }

data ProgramError = FatalError deriving (Eq, Show)

data LoopState r = LoopState {
  _loopStateIterations :: Int,
  _loopStateActivity :: LoopActivity,
  _loopStateTestState :: TestState,
  _loopStatePrograms :: Map Text (RunningProgramState r),
  _loopStateProgramResults :: Map Text (Either ProgramError r)
}

data TestState = TestState {
  _testStateDynamo :: TestDynamoTable,
  _testStateLog :: Seq Text
} deriving (Eq)

instance P.Show TestState where
  show a = 
    "Dynamo: \n" <> P.show (_testStateDynamo a) <> "\n" <> "Log: \n" <> foldl' (\s l -> s <> "\n" <> P.show l) "" (_testStateLog a)

$(makeLenses ''LoopState)

$(makeLenses ''TestState)

emptyTestState :: TestState
emptyTestState = TestState mempty mempty

type InterpreterApp m a r = RandomFailure m => (StateT (LoopState a) m) r

maxIterations :: Int
maxIterations = 100000

isComplete :: InterpreterApp m a Bool
isComplete = do
  allProgramsComplete <- uses loopStatePrograms Map.null
  tooManyIterations <- uses loopStateIterations (> maxIterations)
  allProgramsOverMaxIdle <- uses loopStateActivity allOverMaxIdle
  return $ allProgramsComplete || tooManyIterations || allProgramsOverMaxIdle
  where 
    allOverMaxIdle :: LoopActivity -> Bool 
    allOverMaxIdle (LoopActive _) = False
    allOverMaxIdle (LoopAllIdle status) = Map.filter (>0) status == mempty

addLog :: Text -> InterpreterApp m a ()
addLog m =
  let
    appendMessage = flip (|>) m
  in (loopStateTestState . testStateLog) %= appendMessage

class Monad m => RandomFailure m where
  checkFail :: Double -> m Bool 

instance RandomFailure QC.Gen where
  checkFail failurePercent = do
     r <- QC.choose (0, 99)
     return $ r < failurePercent

instance RandomFailure Identity where
   checkFail _ = return False

potentialFailure :: Double -> InterpreterApp m a r -> InterpreterApp m a r -> InterpreterApp m a r
potentialFailure failurePercent onFailure onSuccess = do
   didFail <- lift $ checkFail failurePercent
   if didFail
     then onFailure
     else onSuccess

writeToDynamo :: DynamoKey -> DynamoValues -> DynamoVersion -> (DynamoWriteResult -> n)  -> InterpreterApp m a n
writeToDynamo key values version next =
  potentialFailure 25 onFailure onSuccess
  where
    onFailure =
      addLog "Random write failure" $> next DynamoWriteFailure
    onSuccess = do
      currentEntry <- Map.lookup key <$> use (loopStateTestState . testStateDynamo)
      let currentVersion = fst <$> currentEntry
      writeVersion version currentVersion
    writeVersion 0 Nothing = performWrite 0
    writeVersion newVersion Nothing = versionFailure newVersion Nothing
    writeVersion v (Just cv)
      | cv == v - 1 = performWrite v
      | otherwise = versionFailure v (Just cv)
    versionFailure newVersion currentVersion = do
       _ <- addLog $ "Wrong version writing: " ++ show newVersion ++ " current: " ++ show currentVersion
       return $ next DynamoWriteWrongVersion
    performWrite newVersion = do
       _ <- addLog $ "Performing write: " ++ show key ++ " " ++ show values ++ " " ++ show version
       _ <- loopStateTestState . testStateDynamo %= Map.insert key (newVersion, values)
       return $ next DynamoWriteSuccess

getReadResult :: DynamoKey -> TestDynamoTable -> Maybe DynamoReadResult
getReadResult key table = do
  (version, values) <- Map.lookup key table
  return $ DynamoReadResult key version values

getUptoItem :: (a -> Bool) -> [a] -> [a]
getUptoItem p =
  unfoldr f
    where f [] = Nothing
          f (x:xs)
            | p x = Nothing
            | otherwise = Just (x, xs)

queryBackward :: Text -> Natural -> Maybe Int64 -> TestDynamoTable -> [DynamoReadResult]
queryBackward streamId maxEvents startEvent table = 
  take (fromIntegral maxEvents) $ reverse $ eventsBeforeStart startEvent 
  where 
    dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber
    eventsBeforeStart Nothing = allEvents
    eventsBeforeStart (Just start) = getUptoItem (\a -> dynamoReadResultToEventNumber a >= start) allEvents
    allEvents = 
      let 
        filteredMap = Map.filterWithKey (\(DynamoKey hashKey _rangeKey) _value -> hashKey == streamId) table
        eventList = (sortOn fst . Map.toList) filteredMap
      in (\(key, (version, values)) -> DynamoReadResult key version values) <$> eventList

scanNeedsPaging :: TestDynamoTable -> [DynamoKey]
scanNeedsPaging = 
   let 
     entryNeedsPaging = HM.member Constants.needsPagingKey . snd
   in Map.keys . Map.filter entryNeedsPaging

setPulseStatus :: Text -> Bool -> InterpreterApp m a ()
setPulseStatus programName isActive = do
  allInactiveState <- uses loopStatePrograms initialAllInactive
  loopStateActivity %= updatePulseStatus (LoopAllIdle allInactiveState) isActive
  where 
    allInactive :: Map Text Bool -> Bool
    allInactive m = Map.filter id m == mempty
    initialAllInactive :: Map Text (RunningProgramState a) -> Map Text Int
    initialAllInactive = fmap runningProgramStateMaxIdle
    updatePulseStatus :: LoopActivity -> Bool -> LoopActivity -> LoopActivity
    updatePulseStatus allInactiveState _ LoopActive { _loopActiveIdleCount = idleCount } =
      let updated = Map.alter (const (Just isActive)) programName idleCount
      in if allInactive updated then allInactiveState else LoopActive updated
    updatePulseStatus _ True LoopAllIdle { _loopAllIdleIterationsRemaining = _idleRemaining } = LoopActive mempty
    updatePulseStatus _ False LoopAllIdle { _loopAllIdleIterationsRemaining = idleRemaining } = LoopAllIdle $ Map.adjust (\x -> x - 1) programName idleRemaining

runCmd :: Text -> DynamoCmdMFree r -> InterpreterApp m r (Either (Either ProgramError r) (DynamoCmdMFree r))
runCmd _ (Free.Pure r) = return $ Left (Right r)
runCmd _ (Free.Free (FatalError' msg)) = const (Left $ Left FatalError) <$> addLog ("Fatal Error" <> msg)
runCmd _ (Free.Free (Wait' _ r)) = Right <$> return r
runCmd _ (Free.Free (QueryBackward' key maxEvents start r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . queryBackward key maxEvents start) 
runCmd _ (Free.Free (WriteToDynamo' key values version r)) = Right <$> writeToDynamo key values version r
runCmd _ (Free.Free (ReadFromDynamo' key r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . getReadResult key)
runCmd _ (Free.Free (Log' _ msg r)) = Right <$> (addLog msg >> return r)
runCmd programId (Free.Free (SetPulseStatus' isActive r)) = Right <$> (setPulseStatus programId isActive >> return r)
runCmd _ (Free.Free (ScanNeedsPaging' r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . scanNeedsPaging)

stepProgram :: Text -> RunningProgramState r -> InterpreterApp m r () 
stepProgram programId ps = do
  cmdResult <- runCmd programId $ runningProgramStateNext ps
  bigBanana cmdResult
  where
    bigBanana :: Either (Either ProgramError r) (DynamoCmdMFree r) -> InterpreterApp m r ()
    bigBanana next = do 
      let result = fmap setNextProgram next
      updateLoopState programId result
    setNextProgram :: DynamoCmdMFree r -> RunningProgramState r
    setNextProgram n = ps { runningProgramStateNext = n }

updateLoopState :: Text -> Either (Either ProgramError r) (RunningProgramState r) -> InterpreterApp m r ()
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
 programs <- use loopStatePrograms
 (programId, programState) <- lift . QC.elements $ Map.toList programs
 stepProgram programId programState

incrimentIterations :: InterpreterApp m a ()
incrimentIterations = loopStateIterations %= (+ 1)

runPrograms :: Map Text (DynamoCmdM a, Int) -> QC.Gen (Map Text (Either ProgramError a), TestState)
runPrograms programs =
  over _2 (view loopStateTestState) <$> runStateT loop initialState
  where
        runningPrograms = fmap (\(p,maxIdleIterations) -> RunningProgramState (Church.fromF p) maxIdleIterations) programs
        initialState = LoopState 0 (LoopActive mempty) emptyTestState runningPrograms mempty
        loop :: InterpreterApp QC.Gen a (Map Text (Either ProgramError a))
        loop = do
          complete <- isComplete
          if complete
             then use loopStateProgramResults
             else iterateApp >> incrimentIterations >> loop

runProgramGenerator :: Text -> DynamoCmdM a -> TestState -> QC.Gen (Either ProgramError a)
runProgramGenerator programId program testState =
  evalStateT (loop $ Right (Church.fromF program)) initialState
  where
        runningPrograms = Map.singleton programId (RunningProgramState (Church.fromF program) 0)
        initialState = LoopState 0 (LoopActive mempty) testState runningPrograms mempty
        loop :: Either (Either ProgramError r) (DynamoCmdMFree r) -> InterpreterApp QC.Gen r (Either ProgramError r)
        loop (Left x) = return x
        loop (Right n) = runCmd programId n >>= loop

runProgram :: Text -> DynamoCmdM a -> TestState -> (Either ProgramError a, LoopState a)
runProgram programId program testState =
  runIdentity $ runStateT (loop $ Right (Church.fromF program)) initialState
  where
        runningPrograms = Map.singleton programId (RunningProgramState (Church.fromF program) 0)
        initialState = LoopState 0 (LoopActive mempty) testState runningPrograms mempty
        loop :: Either (Either ProgramError r) (DynamoCmdMFree r) -> InterpreterApp Identity r (Either ProgramError r)
        loop (Left x) = return x
        loop (Right n) = runCmd programId n >>= loop

evalProgram :: Text -> DynamoCmdM a -> TestState -> (Either ProgramError a)
evalProgram programId program testState = fst $ runProgram programId program testState
execProgram :: Text -> DynamoCmdM a -> TestState -> (LoopState a)
execProgram programId program testState = snd $ runProgram programId program testState
