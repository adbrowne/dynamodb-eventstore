{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}

module DynamoCommands where

import Control.Monad.Free
import Control.Monad.Free.TH

import Data.Int
import qualified Data.Text as T
import qualified Data.ByteString as BS

newtype StreamId = StreamId T.Text deriving (Ord, Eq, Show)
newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
type EventType = String
data EventWriteResult = WriteSuccess | EventExists | WriteError

data DynamoCmd next =
  GetEvent' EventKey (Maybe (EventType, BS.ByteString) -> next) |
  WriteEvent' EventKey EventType BS.ByteString (EventWriteResult -> next) deriving (Functor)

type DynamoCmdM = Free DynamoCmd

makeFree ''DynamoCmd
