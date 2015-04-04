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

runTest :: MonadState FakeEventTable m => EventStoreCmdM a -> m a
runTest = iterM run
  where
    run (Wait' n) = n ()
    run (GetEvent' k f) = f =<< gets (M.lookup k)
    run (WriteEvent' k t v n) = do
      exists <- gets (M.lookup k)
      result <- writeEvent exists k t v
      n result
    run (SetEventPage' k pk n) = do
      let f (et, eb, _) = Just (et, eb, Just pk)
      modify $ M.update f k
      n SetEventPageSuccess
    run (ScanUnpagedEvents' n) = do
      unpaged <- gets (M.filter unpagedEntry)
      n $ M.keys unpaged
    unpagedEntry (_, _, (Just _)) = False
    unpagedEntry (_, _, Nothing) = True
    writeEvent Nothing k t v = do
      modify $ M.insert k (t,v, Nothing)
      return WriteSuccess
    writeEvent (Just _) _ _ _ = do
      return EventExists

evalProgram program = evalState (runTest program) M.empty

execProgram program = execState (runTest program) M.empty
