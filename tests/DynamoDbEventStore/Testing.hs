{-# LANGUAGE FlexibleContexts #-}
module DynamoDbEventStore.Testing where

import           Control.Applicative
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

writeEventToState f (eT, pT) =
   (f eT, pT)

writePageToState f (eT, pT) =
   (eT, f pT)

runCmd :: MonadState FakeState m => EventStoreCmd (m a) -> m a
runCmd (Wait' n) = n ()
runCmd (GetEvent' k f) = f . M.lookup k =<< gets fst
runCmd (WriteEvent' k t v n) = do
  exists <- gets (M.lookup k . fst)
  result <- writeEvent exists k t v
  n result
    where
      writeEvent Nothing k t v = do
        modify $ writeEventToState $ M.insert k  (t, v, Nothing)
        return WriteSuccess
      writeEvent (Just _) _ _ _ =
        return EventExists
runCmd (SetEventPage' k pk n) = do
  let f (et, eb, _) = Just (et, eb, Just pk)
  modify $ writeEventToState $ M.update f k
  n SetEventPageSuccess
runCmd (ScanUnpagedEvents' n) = do
  unpaged <- gets (M.filter unpagedEntry . fst)
  n $ M.keys unpaged
  where
    unpagedEntry (_, _, Just _) = False
    unpagedEntry (_, _, Nothing) = True
runCmd (GetPageEntry' k n) =
  n =<< gets (M.lookup k . snd)
runCmd (WritePageEntry' k PageWriteRequest { expectedStatus = expectedStatus, newStatus = newStatus, newEntries = newEntries } n) = do
  table <- gets snd
  let entryStatus = fst <$> M.lookup k table
  let writeResult = writePage expectedStatus entryStatus table
  modify $ writePageToState (modifyPage writeResult)
  n $ fmap fst writeResult
    where
      doInsert = M.insert k (newStatus, newEntries)
      writePage :: Maybe PageStatus -> Maybe PageStatus -> FakePageTable -> Maybe (PageStatus, FakePageTable)
      writePage Nothing Nothing table = Just (newStatus,  doInsert table)
      writePage (Just c) (Just e) table
        | c == e =  Just (newStatus, doInsert table)
        | otherwise = Nothing
      writePage _ _ _ = Nothing
      modifyPage :: Maybe (PageStatus, FakePageTable) -> FakePageTable -> FakePageTable
      modifyPage (Just (_, pT)) _ = pT
      modifyPage Nothing state = state

runTest :: MonadState FakeState m => EventStoreCmdM a -> m a
runTest = iterM runCmd

evalProgram :: EventStoreCmdM a -> IO a
evalProgram program = return $ evalState (runTest program) (M.empty, M.empty)
