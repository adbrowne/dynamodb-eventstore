module DynamoDbEventStore.ProjectPrelude
  (traceM
  ,module X
  ,NonEmpty(..)
  ,fmap2
  ,Foldable.traverse_
  ) where

import BasicPrelude as X
import GHC.Natural as X
import Control.Monad.Except as X
import Pipes as X (Producer, yield, await, (>->), Pipe)
import Data.List.NonEmpty (NonEmpty(..))
import Data.Foldable as Foldable

import qualified Debug.Trace
import Data.Text as T

{-# WARNING traceM "traceM still in code" #-}
traceM :: Monad m => T.Text -> m ()
traceM = Debug.Trace.traceM . T.unpack

fmap2
    :: (Functor f, Functor f1)
    => (a -> b) -> f (f1 a) -> f (f1 b)
fmap2 = fmap . fmap
