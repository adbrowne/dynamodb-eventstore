{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ViewPatterns #-}

module DynamoDbEventStore.InMemoryQueues
  (Queues
  , Queue
  , emptyQueues
  , newQueue
  , writeToQueue
  , tryReadFromQueue
  ) where

import           BasicPrelude
import           Data.Dynamic
import qualified Data.Sequence as Seq
import           Data.Sequence ((<|),ViewR(..))
import qualified Data.Map.Strict as Map

data Queues = Queues {
  queuesNextIndex :: Int,
  queuesQueueMap :: Map Int (Maybe Dynamic)
}
  deriving (Show)

newtype Queue a = Queue { unQueue :: Int }
  deriving (Show)

emptyQueues :: Queues
emptyQueues = Queues {
  queuesNextIndex = 0,
  queuesQueueMap = mempty }

newQueue :: Queues -> (Queue a, Queues)
newQueue Queues{..} =
  let
    queueKey = Queue { unQueue = queuesNextIndex }
    queues = Queues {
        queuesNextIndex = queuesNextIndex + 1,
        queuesQueueMap = Map.insert queuesNextIndex Nothing queuesQueueMap }
  in (queueKey, queues)

writeToQueue :: Typeable a => Queues -> Queue a -> a -> Queues
writeToQueue queues@Queues{..} Queue{..} item =
  let
    q = queuesQueueMap Map.! unQueue
    updatedQueue = addItem q
  in
    queues { queuesQueueMap = Map.insert unQueue (Just updatedQueue) queuesQueueMap }
  where
    addItem Nothing = toDyn $ Seq.singleton item
    addItem (Just dyn) =
      let
        items = fromDyn dyn (error "InMemoryQueues.writeToQueue Invalid format for queue")
      in toDyn $ item <| items

tryReadFromQueue :: Typeable a => Queues -> Queue a -> (Maybe a, Queues)
tryReadFromQueue queues@Queues{..} Queue{..} =
  let
    q = queuesQueueMap Map.! unQueue
    (maybeItem, updatedQueue) = tryRead q
  in (maybeItem, queues { queuesQueueMap = Map.insert unQueue updatedQueue queuesQueueMap })
  where
    tryRead Nothing = (Nothing, Nothing)
    tryRead (Just dyn) =
      let
        items = fromDyn dyn (error "InMemoryQueues.writeToQueue Invalid format for queue")
        (item, queue') = tryReadSeq items
      in (item, toDyn <$> queue')
    tryReadSeq (Seq.viewr -> Seq.EmptyR) = (Nothing, Nothing)
    tryReadSeq (Seq.viewr -> xs :> x) = (Just x, Just xs)
    tryReadSeq _ = error "match error on Seq"
