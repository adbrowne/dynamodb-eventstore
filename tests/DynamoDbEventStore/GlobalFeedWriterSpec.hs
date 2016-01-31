{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE PartialTypeSignatures #-}

module DynamoDbEventStore.GlobalFeedWriterSpec where

import           Test.Tasty
import           Test.Tasty.QuickCheck((===),testProperty)
import qualified Test.Tasty.QuickCheck as QC
import           Control.Lens
import           Control.Lens.TH
import           Control.Monad.State
import           Control.Monad.Free
import           Control.Monad.Trans.Class
import qualified Data.HashMap.Lazy as HM
import qualified Data.Text             as T
import           EventStoreCommands
import           Data.Foldable
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V

import Network.AWS.DynamoDB
newtype UploadList = UploadList [(T.Text,Int,T.Text)] deriving (Show)

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen[a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

instance QC.Arbitrary T.Text where
  arbitrary = T.pack <$> QC.arbitrary
  shrink xs = T.pack <$> QC.shrink (T.unpack xs)

instance QC.Arbitrary UploadList where
  arbitrary = do
    streams <- cappedList 5
    events <- cappedList 100
    eventsWithStream <- sequence $ map (assignStream streams) events
    return $ UploadList $ numberEvents eventsWithStream
    where
      assignStream streams event = do
        stream <- QC.elements streams
        return (stream, event)
      groupTuple xs = HM.toList $ HM.fromListWith (++) ((\(a,b) -> (a,[b])) <$> xs)
      numberEvents :: [(T.Text, T.Text)] -> [(T.Text, Int, T.Text)]
      numberEvents xs = do
        (stream, events) <- groupTuple xs
        (event, number) <- zip events [0..]
        return (stream, number, event)

dynamoWriteWithRetry :: (T.Text, Int, T.Text) -> DynamoCmdM DynamoWriteResult
dynamoWriteWithRetry (stream, eventNumber, body) = loop DynamoWriteFailure
  where
    values = HM.fromList
      [ ("Body", set avS (Just body) attributeValue),
        ("NeedsPaging", set avS (Just "True") attributeValue) ]
    loop :: DynamoWriteResult -> DynamoCmdM DynamoWriteResult
    loop DynamoWriteFailure = do
      writeToDynamo' (DynamoKey stream eventNumber) values Nothing >>= loop
    loop r = return r

type TestDynamoTable = Map.Map DynamoKey (Int, DynamoValues)

data RunningProgramState r = RunningProgramState {
  runningProgramStateNext :: DynamoCmdM r
}

data LoopState r = LoopState {
  _loopStateIterations :: Int,
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

maxIterations = 100000
isComplete :: InterpreterApp a Bool
isComplete = do
  allProgramsComplete <- uses loopStatePrograms Map.null
  tooManyIterations <- uses loopStateIterations (> maxIterations)
  return $ allProgramsComplete || tooManyIterations

addLog :: T.Text -> LoopState r -> LoopState r
addLog m =
  let
    appendMessage = flip V.snoc m
  in over (loopStateTestState . testStateLog) appendMessage

stepProgram :: RunningProgramState r -> InterpreterApp r (Either () (RunningProgramState r))
stepProgram ps = do
  result <- runCmd $ runningProgramStateNext ps
  let toReturn = fmap (setNextProgram ps) result
  return toReturn
  where
    setNextProgram :: RunningProgramState r -> DynamoCmdM r -> RunningProgramState r
    setNextProgram ps n = ps { runningProgramStateNext = n }
    runCmd :: DynamoCmdM r -> InterpreterApp r (Either () (DynamoCmdM r))
    runCmd (Pure x) = return $ Left ()
    runCmd (Free (WriteToDynamo' _ _ _ r)) = do
      _ <- modify $ addLog "Writing to dynamo"
      return $ Right (r DynamoWriteSuccess)
    runCmd _ = undefined

updateLoopState :: T.Text -> (Either () (RunningProgramState r)) -> InterpreterApp r ()
updateLoopState programName result =
  loopStatePrograms %= (updateProgramEntry result)
  where
    updateProgramEntry (Left ())        = Map.delete programName
    updateProgramEntry (Right newState) = Map.adjust (const newState) programName

iterateApp :: InterpreterApp a ()
iterateApp = do
 programs <- use loopStatePrograms
 (programId, programState) <- lift . QC.elements $ Map.toList programs
 stepResult' <- stepProgram programState
 _ <- updateLoopState programId stepResult'
 return ()

runPrograms :: Map.Map T.Text (DynamoCmdM a) -> QC.Gen ([a],TestState)
runPrograms programs =
  fmap (over _2 (view loopStateTestState)) $ runStateT loop initialState
  where
        runningPrograms = fmap (RunningProgramState) programs
        initialState = LoopState 0 (TestState Map.empty V.empty) runningPrograms
        loop :: InterpreterApp a [a]
        loop = do
          complete <- isComplete
          if complete
             then return []
             else iterateApp >> loop

publisher :: [(T.Text,Int,T.Text)] -> DynamoCmdM ()
publisher [] = return ()
publisher xs = forM_ xs dynamoWriteWithRetry

prop_EventShouldAppearInGlobalFeedInStreamOrder :: UploadList -> QC.Property
prop_EventShouldAppearInGlobalFeedInStreamOrder (UploadList uploadList) =
  let
    programs = Map.singleton "Publisher" $ publisher uploadList
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = V.length (testState ^. testStateLog) === length uploadList

tests :: [TestTree]
tests = [
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder
  ]
