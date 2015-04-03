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

runItem :: FakeEventTable -> PostEventRequest -> FakeEventTable
runItem state (PostEventRequest sId v d) =
  let
   (_, s) = runState (runTest writeItem) state
  in s
  where
      writeItem = do
        writeEvent' (EventKey (StreamId (TL.toStrict sId),v)) "SomeEventType" (BL.toStrict d)

