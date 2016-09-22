module DynamoDbEventStore.ProjectPrelude
  (traceM) where

import BasicPrelude

import qualified Debug.Trace
import Data.Text as T

traceM :: Monad m => T.Text -> m ()
traceM = Debug.Trace.traceM . T.unpack
