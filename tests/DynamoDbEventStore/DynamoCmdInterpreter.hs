{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE FlexibleContexts #-}

module DynamoDbEventStore.DynamoCmdInterpreter where

import           Control.Lens
import           Control.Monad.State
import           Data.Functor (($>))
import qualified Data.HashMap.Lazy as HM
import qualified Control.Monad.Free.Church as Church
import qualified Test.Tasty.QuickCheck as QC
import qualified Control.Monad.Free as Free
import qualified Data.Map.Strict as Map
import qualified Data.Text             as T
import qualified Data.Vector as V

import qualified DynamoDbEventStore.Constants as Constants
import           DynamoDbEventStore.EventStoreCommands

type DynamoCmdMFree = Free.Free DynamoCmd

type TestDynamoTable = Map.Map DynamoKey (Int, DynamoValues)

data RunningProgramState r = RunningProgramState {
  runningProgramStateNext :: DynamoCmdMFree r,
  runningProgramStateMaxIdle :: Int
}

data LoopActivity = 
  LoopActive { _loopActiveIdleCount :: Map.Map T.Text Bool } |
  LoopAllIdle { _loopAllIdleIterationsRemaining :: Map.Map T.Text Int }

data LoopState r = LoopState {
  _loopStateIterations :: Int,
  _loopStateActivity :: LoopActivity,
  _loopStateTestState :: TestState,
  _loopStatePrograms :: Map.Map T.Text (RunningProgramState r)
}

data TestState = TestState {
  _testStateDynamo :: TestDynamoTable,
  _testStateLog :: V.Vector T.Text
} deriving (Show, Eq)

$(makeLenses ''LoopState)

$(makeLenses ''TestState)

type InterpreterApp a = (StateT (LoopState a) QC.Gen)

maxIterations :: Int
maxIterations = 100000

isComplete :: InterpreterApp a Bool
isComplete = do
  allProgramsComplete <- uses loopStatePrograms Map.null
  tooManyIterations <- uses loopStateIterations (> maxIterations)
  allProgramsOverMaxIdle <- uses loopStateActivity allOverMaxIdle
  return $ allProgramsComplete || tooManyIterations || allProgramsOverMaxIdle
  where 
    allOverMaxIdle :: LoopActivity -> Bool 
    allOverMaxIdle (LoopActive _) = False
    allOverMaxIdle (LoopAllIdle status) = Map.filter (>0) status == Map.empty

addLog :: T.Text -> InterpreterApp a ()
addLog m =
  let
    appendMessage = flip V.snoc m
  in (loopStateTestState . testStateLog) %= appendMessage

addLogS :: String -> InterpreterApp a ()
addLogS = addLog . T.pack

potentialFailure :: Double -> InterpreterApp a r -> InterpreterApp a r -> InterpreterApp a r
potentialFailure failurePercent onFailure onSuccess = do
   r <- lift $ QC.choose (0, 99)
   if r < failurePercent
     then onFailure
     else onSuccess

writeToDynamo :: DynamoKey -> DynamoValues -> DynamoVersion -> (DynamoWriteResult -> n)  -> InterpreterApp a n
writeToDynamo key values version next =
  potentialFailure 0 onFailure onSuccess -- todo: reinstate failure
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
       _ <- addLogS $ "Wrong version writing: " ++ show newVersion ++ " current: " ++ show currentVersion
       return $ next DynamoWriteWrongVersion
    performWrite newVersion = do
       _ <- addLogS $ "Performing write: " ++ show key ++ " " ++ show values ++ " " ++ show version
       _ <- loopStateTestState . testStateDynamo %= Map.insert key (newVersion, values)
       return $ next DynamoWriteSuccess

getReadResult :: DynamoKey -> TestDynamoTable -> Maybe DynamoReadResult
getReadResult key table = do
  (version, values) <- Map.lookup key table
  return $ DynamoReadResult key version values

scanNeedsPaging :: TestDynamoTable -> [DynamoKey]
scanNeedsPaging = 
   let 
     entryNeedsPaging = HM.member Constants.needsPagingKey . snd
   in Map.keys . Map.filter entryNeedsPaging

setPulseStatus :: T.Text -> Bool -> InterpreterApp a ()
setPulseStatus programName isActive = do
  allInactiveState <- uses loopStatePrograms initialAllInactive
  loopStateActivity %= updatePulseStatus (LoopAllIdle allInactiveState) isActive
  where 
    allInactive :: Map.Map T.Text Bool -> Bool
    allInactive m = Map.filter id m == Map.empty
    initialAllInactive :: Map.Map T.Text (RunningProgramState a) -> Map.Map T.Text Int
    initialAllInactive = fmap runningProgramStateMaxIdle
    updatePulseStatus :: LoopActivity -> Bool -> LoopActivity -> LoopActivity
    updatePulseStatus allInactiveState _ LoopActive { _loopActiveIdleCount = idleCount } =
      let updated = Map.alter (const (Just isActive)) programName idleCount
      in if allInactive updated then allInactiveState else LoopActive updated
    updatePulseStatus _ True LoopAllIdle { _loopAllIdleIterationsRemaining = _idleRemaining } = LoopActive Map.empty
    updatePulseStatus _ False LoopAllIdle { _loopAllIdleIterationsRemaining = idleRemaining } = LoopAllIdle $ Map.adjust (\x -> x - 1) programName idleRemaining

runCmd :: T.Text -> DynamoCmdMFree r -> InterpreterApp r (Either (Maybe r) (DynamoCmdMFree r))
runCmd _ (Free.Pure r) = return $ Left (Just r)
runCmd _ (Free.Free (FatalError' msg)) = const (Left Nothing) <$> addLog (T.append "Fatal Error" msg)
runCmd _ (Free.Free QueryBackward'{}) = error "todo: implement QueryBackward'"
runCmd _ (Free.Free (WriteToDynamo' key values version r)) = Right <$> writeToDynamo key values version r
runCmd _ (Free.Free (ReadFromDynamo' key r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . getReadResult key)
runCmd _ (Free.Free (Log' _ msg r)) = Right <$> (addLog msg >> return r)
runCmd programId (Free.Free (SetPulseStatus' isActive r)) = Right <$> (setPulseStatus programId isActive >> return r)
runCmd _ (Free.Free (ScanNeedsPaging' r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . scanNeedsPaging)

stepProgram :: T.Text -> RunningProgramState r -> InterpreterApp r () 
stepProgram programId ps = do
  cmdResult <- runCmd programId $ runningProgramStateNext ps
  bigBanana cmdResult
  where
    bigBanana :: Either (Maybe r) (DynamoCmdMFree r) -> InterpreterApp r ()
    bigBanana next = do 
      let result = fmap setNextProgram next
      updateLoopState programId result
    setNextProgram :: DynamoCmdMFree r -> RunningProgramState r
    setNextProgram n = ps { runningProgramStateNext = n }

updateLoopState :: T.Text -> Either (Maybe r) (RunningProgramState r) -> InterpreterApp r ()
updateLoopState programName result =
  loopStatePrograms %= updateProgramEntry result
  where
    updateProgramEntry (Left _)        = Map.delete programName
    updateProgramEntry (Right newState) = Map.adjust (const newState) programName

iterateApp :: InterpreterApp a ()
iterateApp = do
 programs <- use loopStatePrograms
 (programId, programState) <- lift . QC.elements $ Map.toList programs
 stepProgram programId programState

incrimentIterations :: InterpreterApp a ()
incrimentIterations = loopStateIterations %= (+ 1)

runPrograms :: Map.Map T.Text (DynamoCmdM a, Int) -> QC.Gen ([a],TestState)
runPrograms programs =
  over _2 (view loopStateTestState) <$> runStateT loop initialState
  where
        runningPrograms = fmap (\(p,maxIdleIterations) -> RunningProgramState (Church.fromF p) maxIdleIterations) programs
        initialState = LoopState 0 (LoopActive Map.empty) (TestState Map.empty V.empty) runningPrograms
        loop :: InterpreterApp a [a]
        loop = do
          complete <- isComplete
          if complete
             then return []
             else iterateApp >> incrimentIterations >> loop

runProgram :: T.Text -> DynamoCmdMFree a -> TestState -> QC.Gen (Maybe a)
runProgram programId program testState =
  evalStateT (loop $ Right program) initialState
  where
        runningPrograms = Map.singleton programId (RunningProgramState program 0)
        initialState = LoopState 0 (LoopActive Map.empty) testState runningPrograms
        loop :: Either (Maybe r) (DynamoCmdMFree r) -> InterpreterApp r (Maybe r)
        loop (Left x) = return x
        loop (Right n) = runCmd programId n >>= loop
