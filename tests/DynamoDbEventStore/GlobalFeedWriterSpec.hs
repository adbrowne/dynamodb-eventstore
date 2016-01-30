{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DynamoDbEventStore.GlobalFeedWriterSpec (tests) where

import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Control.Lens (set)
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
cappedList :: Arbitrary a => Int -> Gen[a]
cappedList maxLength = listOf1 arbitrary `suchThat` ((< maxLength) . length)

instance Arbitrary T.Text where
  arbitrary = T.pack <$> arbitrary
  shrink xs = T.pack <$> shrink (T.unpack xs)

instance Arbitrary UploadList where
  arbitrary = do
    streams <- cappedList 5
    events <- cappedList 100
    eventsWithStream <- sequence $ map (assignStream streams) events
    return $ UploadList $ numberEvents eventsWithStream
    where
      assignStream streams event = do
        stream <- elements streams
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
  loopStateIterations :: Int,
  loopStateTestState :: TestState,
  loopStatePrograms :: Map.Map T.Text (RunningProgramState r)
}

data TestState = TestState {
  testStateDynamo :: TestDynamoTable,
  testStateLog :: V.Vector T.Text
} deriving (Show, Eq)

type InterpreterApp a = (StateT (LoopState a) Gen)

maxIterations = 100000
isComplete :: InterpreterApp a Bool
isComplete = do
  allProgramsComplete <- Map.null <$> gets loopStatePrograms
  tooManyIterations <- (> maxIterations) <$> gets loopStateIterations
  return $ allProgramsComplete || tooManyIterations

addLog :: T.Text -> LoopState r -> LoopState r
addLog m state =
  let
    testState = loopStateTestState state
    log = testStateLog testState
    log' = V.snoc log m
  in state { loopStateTestState = testState { testStateLog = log' } }

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
updateLoopState programName (Left ()) = modify removeProgram
  where removeProgram state =
          let
            programs = loopStatePrograms state
            programs' = Map.delete programName programs
          in state { loopStatePrograms = programs' }
updateLoopState programName (Right newState) = modify updateProgram
  where updateProgram state =
          let
            programs = loopStatePrograms state
            programs' = Map.adjust (const newState) programName programs
          in state { loopStatePrograms = programs' }

iterateApp :: InterpreterApp a ()
iterateApp = do
 programs <- gets loopStatePrograms
 (programId, programState) <- lift . elements $ Map.toList programs
 stepResult' <- stepProgram programState
 _ <- updateLoopState programId stepResult'
 return ()

runPrograms :: Map.Map T.Text (DynamoCmdM a) -> Gen ([a],TestState)
runPrograms programs =
  fmap (\(as,ls) -> (as, loopStateTestState ls)) $ runStateT loop initialState
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

prop_EventShouldAppearInGlobalFeedInStreamOrder :: UploadList -> Property
prop_EventShouldAppearInGlobalFeedInStreamOrder (UploadList xs) =
  let
    programs = Map.singleton "Publisher" $ publisher xs
  in forAll (runPrograms programs) check
     where
       check (_, testState) = V.length (testStateLog testState) === length xs

tests :: [TestTree]
tests = [
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder
  ]
