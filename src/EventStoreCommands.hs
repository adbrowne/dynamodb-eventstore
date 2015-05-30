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
type EventType = T.Text
type PageKey = (Int, Int) -- (Partition, PageNumber)
data EventWriteResult = WriteSuccess | EventExists | WriteError deriving (Eq, Show)
type EventReadResult = Maybe (EventType, BS.ByteString, Maybe PageKey)
data SetEventPageResult = SetEventPageSuccess | SetEventPageError
data PageStatus = Version Int | Full | Verified deriving (Eq, Show, Read)

data PageWriteRequest = PageWriteRequest {
      expectedStatus :: Maybe PageStatus
      , newStatus    :: PageStatus
      , newEntries   :: [EventKey]
}

-- Low level event store commands
-- should map almost one to one with dynamodb operations
data EventStoreCmd next =
  GetEvent'
    EventKey
    (EventReadResult -> next) |
  WriteEvent'
    EventKey
    EventType
    BS.ByteString
    (EventWriteResult -> next) |
  Wait'
    (() -> next) |
  SetEventPage'
    EventKey
    PageKey
    (SetEventPageResult -> next) |
  WritePageEntry'
    PageKey
    PageWriteRequest
    (Maybe PageStatus -> next) |
  GetPageEntry'
    PageKey
    (Maybe (PageStatus, [EventKey]) -> next) |
  ScanUnpagedEvents'
    ([EventKey] -> next)
  deriving (Functor) -- todo support paging

type EventStoreCmdM = Free EventStoreCmd

makeFree ''EventStoreCmd
