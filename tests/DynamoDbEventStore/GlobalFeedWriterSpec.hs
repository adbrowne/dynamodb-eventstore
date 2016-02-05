{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE FlexibleContexts #-}

module DynamoDbEventStore.GlobalFeedWriterSpec where

import           Data.List
import           Data.Maybe
import           Test.Tasty
import           Test.Tasty.QuickCheck((===),testProperty)
import qualified Test.Tasty.QuickCheck as QC
import           Control.Applicative
import           Control.Lens
import           Control.Monad.State
import           Control.Monad.Free
import qualified Data.HashMap.Lazy as HM
import qualified Data.Text             as T
import           Data.Functor (($>))
import           EventStoreCommands
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import qualified Data.Aeson as Aeson

import Network.AWS.DynamoDB
type UploadItem = (T.Text,Int,T.Text)
newtype UploadList = UploadList [UploadItem] deriving (Show)

-- Generateds a list of length between 1 and maxLength
cappedList :: QC.Arbitrary a => Int -> QC.Gen [a]
cappedList maxLength = QC.listOf1 QC.arbitrary `QC.suchThat` ((< maxLength) . length)

instance QC.Arbitrary T.Text where
  arbitrary = T.pack <$> QC.arbitrary
  shrink xs = T.pack <$> QC.shrink (T.unpack xs)

instance QC.Arbitrary UploadList where
  arbitrary = do
    streams <- cappedList 5
    events <- cappedList 100
    eventsWithStream <- mapM (assignStream streams) events
    return $ UploadList $ numberEvents eventsWithStream
    where
      assignStream streams event = do
        stream <- QC.elements streams
        return (stream, event)
      groupTuple xs = HM.toList $ HM.fromListWith (++) ((\(a,b) -> (a,[b])) <$> xs)
      numberEvents :: [(T.Text, T.Text)] -> [UploadItem]
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
    loop DynamoWriteFailure =
      writeToDynamo' (DynamoKey stream eventNumber) values Nothing >>= loop
    loop r = return r

type TestDynamoTable = Map.Map DynamoKey (Int, DynamoValues)

data FeedEntry = FeedEntry {
  feedEntryStream :: T.Text,
  feedEntryNumber :: Int
} deriving (Eq, Show)

instance QC.Arbitrary FeedEntry where
  arbitrary = do
    stream <- QC.arbitrary
    number <- QC.arbitrary
    return $ FeedEntry stream number

instance Aeson.FromJSON FeedEntry where
    parseJSON (Aeson.Object v) = FeedEntry <$>
                           v Aeson..: "s" <*>
                           v Aeson..: "n"
    parseJSON _          = empty

instance Aeson.ToJSON FeedEntry where
    toJSON (FeedEntry stream number) =
        Aeson.object ["s" Aeson..= stream, "n" Aeson..= number]

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

maxIterations :: Int
maxIterations = 100000

isComplete :: InterpreterApp a Bool
isComplete = do
  allProgramsComplete <- uses loopStatePrograms Map.null
  tooManyIterations <- uses loopStateIterations (> maxIterations)
  return $ allProgramsComplete || tooManyIterations

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
  potentialFailure 25 onFailure onSuccess
  where
    onFailure =
      addLog "Random write failure" $> next DynamoWriteFailure
    onSuccess = do
      currentEntry <- Map.lookup key <$> use (loopStateTestState . testStateDynamo)
      let currentVersion = fst <$> currentEntry
      writeVersion version currentVersion
    writeVersion Nothing Nothing = performWrite 0
    writeVersion (Just v) (Just cv)
      | cv == v - 1 = performWrite v
      | otherwise = versionFailure (Just v) (Just cv)
    writeVersion newVersion currentVersion = versionFailure newVersion currentVersion
    versionFailure newVersion currentVersion = do
       _ <- addLogS $ "Wrong version writing: " ++ show newVersion ++ " current: " ++ show currentVersion
       return $ next DynamoWriteWrongVersion
    performWrite newVersion = do
       _ <- addLogS $ "Performing write: " ++ show key ++ " " ++ show values ++ " " ++ show version
       _ <- loopStateTestState . testStateDynamo %= (Map.insert key (newVersion, values))
       return $ next DynamoWriteSuccess

getReadResult :: DynamoKey -> TestDynamoTable -> Maybe DynamoReadResult
getReadResult key table = do
  (version, values) <- Map.lookup key table
  return $ DynamoReadResult key version values

stepProgram :: RunningProgramState r -> InterpreterApp r (Either () (RunningProgramState r))
stepProgram ps = do
  result <- runCmd $ runningProgramStateNext ps
  let toReturn = fmap setNextProgram result
  return toReturn
  where
    setNextProgram :: DynamoCmdM r -> RunningProgramState r
    setNextProgram n = ps { runningProgramStateNext = n }
    runCmd :: DynamoCmdM r -> InterpreterApp r (Either () (DynamoCmdM r))
    runCmd (Pure _) = return $ Left ()
    runCmd (Free (WriteToDynamo' key values version r)) = Right <$> writeToDynamo key values version r
    runCmd (Free (ReadFromDynamo' key r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . (getReadResult key))
    runCmd _ = undefined -- todo

updateLoopState :: T.Text -> Either () (RunningProgramState r) -> InterpreterApp r ()
updateLoopState programName result =
  loopStatePrograms %= updateProgramEntry result
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
  over _2 (view loopStateTestState) <$> runStateT loop initialState
  where
        runningPrograms = fmap RunningProgramState programs
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

globalFeedFromTestDynamoTable :: TestDynamoTable -> Map.Map T.Text (V.Vector Int)
globalFeedFromTestDynamoTable testTable =
  foldl' acc Map.empty (getPagedEventOrder testTable)
  where
    getPagedEventOrder :: TestDynamoTable -> [(T.Text, Int)]
    getPagedEventOrder dynamoTable =
      let
        feedEntryToTuple (FeedEntry stream number) = (stream, number)
        readPageValues :: (DynamoKey, (Int, DynamoValues)) -> [FeedEntry]
        readPageValues (_, (_, values)) = fromMaybe [] $
          view (ix "body" . avB) values >>= Aeson.decodeStrict
        pageEntries = Map.toList dynamoTable &
                      filter (\(DynamoKey stream _, _) -> T.isPrefixOf "$page" stream) &
                      sortOn fst >>=
                      readPageValues
      in feedEntryToTuple <$> pageEntries
    acc :: Map.Map T.Text (V.Vector Int) -> (T.Text, Int) -> Map.Map T.Text (V.Vector Int)
    acc s (stream, number) =
      let newValue = maybe (V.singleton number) (`V.snoc` number) $ Map.lookup stream s
      in Map.insert stream newValue s

globalFeedFromUploadList :: [UploadItem] -> Map.Map T.Text (V.Vector Int)
globalFeedFromUploadList =
  foldl' acc Map.empty
  where
    acc :: Map.Map T.Text (V.Vector Int) -> UploadItem -> Map.Map T.Text (V.Vector Int)
    acc s (stream, number, _) =
      let newValue = maybe (V.singleton number) (`V.snoc` number) $ Map.lookup stream s
      in Map.insert stream newValue s

prop_EventShouldAppearInGlobalFeedInStreamOrder :: UploadList -> QC.Property
prop_EventShouldAppearInGlobalFeedInStreamOrder (UploadList uploadList) =
  let
    programs = Map.singleton "Publisher" $ publisher uploadList
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = globalFeedFromTestDynamoTable (testState ^. testStateDynamo) === globalFeedFromUploadList uploadList

tests :: [TestTree]
tests = [
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder,
      testProperty "Can round trip FeedEntry via JSON" (\(a :: FeedEntry) -> (Aeson.decode . Aeson.encode) a === Just a)

  ]
