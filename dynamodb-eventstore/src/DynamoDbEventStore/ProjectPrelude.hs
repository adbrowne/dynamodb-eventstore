module DynamoDbEventStore.ProjectPrelude
  (traceM
  ,module X
  ,NonEmpty(..)
  ) where

import BasicPrelude as X
import GHC.Natural as X
import Control.Monad.Except as X
import Pipes as X (Producer, yield, await, (>->), Pipe)
import Data.List.NonEmpty (NonEmpty(..))

import qualified Debug.Trace
import Data.Text as T

{-# WARNING traceM "traceM still in code" #-}
traceM :: Monad m => T.Text -> m ()
traceM = Debug.Trace.traceM . T.unpack
