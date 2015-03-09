{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}

module EventStoreCommands where

import Control.Monad.Free
import Control.Monad.Free.TH

import Data.Int
import qualified Data.Text as T
import qualified Data.ByteString as BS

newtype StreamId = StreamId T.Text deriving (Ord, Eq, Show)
newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
type EventType = String
type PageKey = (Int, Int) -- (Partition, PageNumber)
data EventWriteResult = WriteSuccess | EventExists | WriteError
data SetEventPageResult = SetEventPageSuccess | SetEventPageError
data PageStatus = Version Int | Full | Verified

data PageWriteRequest = PageWriteRequest {
      expectedStatus :: PageStatus
      , newStatus    :: PageStatus
      , newEntries   :: [EventKey]
}

-- Low level event store commands
-- should map almost one to one with dynamodb operations
data EventStoreCmd next =
  EnsurePartitionCount' Int (Maybe Int -> next) |
  GetEvent' EventKey (Maybe (EventType, BS.ByteString, Maybe PageKey) -> next) |
  WriteEvent' EventKey EventType BS.ByteString (EventWriteResult -> next) |
  SetEventPage' EventKey PageKey (SetEventPageResult -> next) |
  GetPageEntry' PageKey (Maybe (PageStatus, [EventKey]) -> next) |
  WritePageEntry' PageKey PageWriteRequest (Maybe PageStatus -> next) deriving (Functor)

type EventStoreCmdM = Free EventStoreCmd

makeFree ''EventStoreCmd
