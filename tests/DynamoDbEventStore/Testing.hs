{-# LANGUAGE FlexibleContexts #-}
module DynamoDbEventStore.Testing where

import           Control.Monad.Free
import           Control.Monad.State
import           Data.Map                (Map)
import qualified Data.Map                as M
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text.Lazy          as TL
import           EventStoreActions
import           EventStoreCommands

type FakeEventTable = Map EventKey (EventType, BS.ByteString, Maybe PageKey)
type FakePageTable = Map PageKey (PageStatus, [EventKey])

type FakeState = (FakeEventTable, FakePageTable)

emptyTestState :: FakeState
emptyTestState = (M.empty, M.empty)

--writeEventToState :: EventKey -> (EventType, BS.ByteString, Maybe PageKey) -> FakeState -> FakeState
writeEventToState f (eT, pT) =
   (f eT, pT)

runCmd :: MonadState FakeState m => EventStoreCmd (m a) -> m a
runCmd (Wait' n) = n ()
runCmd (GetEvent' k f) = f . (M.lookup k) =<< gets fst
runCmd (WriteEvent' k t v n) = do
  exists <- gets (M.lookup k . fst)
  result <- writeEvent exists k t v
  n result
    where
      writeEvent Nothing k t v = do
        modify $ writeEventToState $ M.insert k  (t, v, Nothing)
        return WriteSuccess
      writeEvent (Just _) _ _ _ = do
        return EventExists
runCmd (SetEventPage' k pk n) = do
  let f (et, eb, _) = Just (et, eb, Just pk)
  modify $ writeEventToState $ M.update f k
  n SetEventPageSuccess
runCmd (ScanUnpagedEvents' n) = do
  unpaged <- gets (M.filter unpagedEntry . fst)
  n $ M.keys unpaged
  where
    unpagedEntry (_, _, (Just _)) = False
    unpagedEntry (_, _, Nothing) = True

runTest :: MonadState FakeState m => EventStoreCmdM a -> m a
runTest = iterM runCmd

evalProgram program = evalState (runTest program) (M.empty, M.empty)

execProgram program = execState (runTest program) (M.empty, M.empty)
