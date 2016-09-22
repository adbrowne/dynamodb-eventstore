{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE FlexibleContexts           #-}

module DynamoDbEventStore.EventStoreQueries
  (readStreamProducer,
  dynamoReadResultToEventNumber,
  streamEntryProducer) where

import BasicPrelude
import GHC.Natural
import Pipes (Producer,yield,(>->))
import Control.Monad.Except
import qualified Pipes.Prelude as P

import DynamoDbEventStore.EventStoreCommands (MonadEsDsl,queryTable)
import DynamoDbEventStore.Types (QueryDirection,StreamId(..),DynamoReadResult(..),DynamoKey(..),EventStoreActionError)
import qualified DynamoDbEventStore.Constants          as Constants
import DynamoDbEventStore.Storage.StreamItem (StreamEntry, dynamoReadResultToStreamEntry)

dynamoReadResultToEventNumber :: DynamoReadResult -> Int64
dynamoReadResultToEventNumber (DynamoReadResult (DynamoKey _key eventNumber) _version _values) = eventNumber

readStreamProducer :: (MonadEsDsl m) => QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer DynamoReadResult m ()
readStreamProducer direction (StreamId streamId) startEvent batchSize = do
  (firstBatch :: [DynamoReadResult]) <- lift $ queryTable direction (Constants.streamDynamoKeyPrefix <> streamId) batchSize startEvent
  yieldResultsAndLoop firstBatch
  where
    yieldResultsAndLoop [] = return ()
    yieldResultsAndLoop [readResult] = do
      yield readResult
      let lastEventNumber = dynamoReadResultToEventNumber readResult
      readStreamProducer direction (StreamId streamId) (Just lastEventNumber) batchSize
    yieldResultsAndLoop (x:xs) = do
      yield x
      yieldResultsAndLoop xs

streamEntryProducer :: (MonadEsDsl m, MonadError EventStoreActionError m ) => QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer StreamEntry m ()
streamEntryProducer direction streamId startEvent batchSize =
  let source = readStreamProducer direction streamId startEvent batchSize
  in source >-> P.mapM dynamoReadResultToStreamEntry
