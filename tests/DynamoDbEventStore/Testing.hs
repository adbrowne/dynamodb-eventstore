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
    run (GetEvent' k f) = f =<< gets (M.lookup k)
    run (WriteEvent' k t v n) = do
      modify $ M.insert k (t,v, Nothing)
      n WriteSuccess
    run (SetEventPage' k pk n) = do
      let f (et, eb, _) = Just (et, eb, Just pk)
      modify $ M.update f k
      n SetEventPageSuccess

evalProgram program = evalState (runTest program) M.empty

execProgram program = execState (runTest program) M.empty
