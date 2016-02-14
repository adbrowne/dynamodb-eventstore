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
import qualified Control.Monad.Free.Church as Church
import qualified Control.Monad.Free as Free
import qualified Data.HashMap.Lazy as HM
import qualified Data.Text             as T
import           Data.Functor (($>))
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import qualified Data.Aeson as Aeson
import           Network.AWS.DynamoDB

import           EventStoreCommands
import qualified GlobalFeedWriter as GlobalFeedWriter
import qualified DynamoDbEventStore.Constants as Constants

type DynamoCmdMFree = Free.Free DynamoCmd
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
        (Constants.needsPagingKey, set avS (Just "True") attributeValue) ]
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
  runningProgramStateNext :: DynamoCmdMFree r,
  runningProgramStateMaxIdle :: Int
}

data LoopActivity = 
  LoopActive { _loopActiveIdleCount :: Map.Map T.Text Int } |
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
setPulseStatus programName isActive =
  loopStateActivity %= updatePulseStatus isActive
  where 
    alterIdleCount :: Bool -> Maybe Int -> Maybe Int
    alterIdleCount True _ = Just 0
    alterIdleCount False Nothing = Just 1
    alterIdleCount False (Just idleCount) = Just $ idleCount + 1
    updatePulseStatus :: Bool -> LoopActivity -> LoopActivity
    updatePulseStatus _ LoopActive { _loopActiveIdleCount = idleCount } = LoopActive $ Map.alter (alterIdleCount isActive) programName idleCount
    updatePulseStatus True LoopAllIdle { _loopAllIdleIterationsRemaining = idleRemaining } = LoopActive Map.empty
    updatePulseStatus False LoopAllIdle { _loopAllIdleIterationsRemaining = idleRemaining } = LoopAllIdle $ Map.adjust (\x -> x - 1) programName idleRemaining

stepProgram :: T.Text -> RunningProgramState r -> InterpreterApp r () 
stepProgram programId ps = do
  cmdResult <- runCmd $ runningProgramStateNext ps
  bigBanana cmdResult
  where
    bigBanana :: Either () (DynamoCmdMFree r) -> InterpreterApp r ()
    bigBanana next = do 
      let result = fmap setNextProgram next
      updateLoopState programId result
    setNextProgram :: DynamoCmdMFree r -> RunningProgramState r
    setNextProgram n = ps { runningProgramStateNext = n }
    runCmd :: DynamoCmdMFree r -> InterpreterApp r (Either () (DynamoCmdMFree r))
    runCmd (Free.Pure _) = return $ Left ()
    runCmd (Free.Free (FatalError' msg)) = Left <$> (addLog $ T.append "Fatal Error" msg)
    runCmd (Free.Free (WriteToDynamo' key values version r)) = Right <$> writeToDynamo key values version r
    runCmd (Free.Free (ReadFromDynamo' key r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . (getReadResult key))
    runCmd (Free.Free (Log' _ msg r)) = Right <$> (addLog msg >> return r)
    runCmd (Free.Free (SetPulseStatus' isActive r)) = Right <$> (setPulseStatus programId isActive >> return r)
    runCmd (Free.Free (ScanNeedsPaging' r)) = Right <$> uses (loopStateTestState . testStateDynamo) (r . scanNeedsPaging)

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

publisher :: [(T.Text,Int,T.Text)] -> DynamoCmdM ()
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
    programs = Map.fromList [
      ("Publisher", (publisher uploadList,100)),
      ("GlobalFeedWriter1", (GlobalFeedWriter.main, 100)) ]
  in QC.forAll (runPrograms programs) check
     where
       check (_, testState) = globalFeedFromTestDynamoTable (testState ^. testStateDynamo) === globalFeedFromUploadList uploadList

tests :: [TestTree]
tests = [
      testProperty "Global Feed preserves stream order" prop_EventShouldAppearInGlobalFeedInStreamOrder,
      testProperty "Can round trip FeedEntry via JSON" (\(a :: FeedEntry) -> (Aeson.decode . Aeson.encode) a === Just a)

  ]
