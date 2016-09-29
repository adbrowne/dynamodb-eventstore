module DynamoDbEventStore
  (streamEventsProducer
  ,globalEventsProducer
  ,EventStore)
where

import DynamoDbEventStore.ProjectPrelude
import qualified DynamoDbEventStore.Streams as Streams
import DynamoDbEventStore.Types
    (RecordedEvent(..),QueryDirection,StreamId,EventStoreActionError,GlobalFeedPosition)
import DynamoDbEventStore.AmazonkaImplementation (RuntimeEnvironment, InterpreterError, MyAwsM(..))
import           DynamoDbEventStore.Storage.StreamItem (EventEntry(..))
import Control.Monad.Trans.AWS
import Control.Monad.Trans.Resource
import Control.Monad.Morph

data EventStoreError =
  EventStoreErrorInterpreter InterpreterError
  | EventStoreErrorAction EventStoreActionError

type EventStore = ExceptT EventStoreError (AWST' RuntimeEnvironment (ResourceT IO))

hoistDsl
  :: (ExceptT EventStoreActionError MyAwsM) a -> (ExceptT EventStoreError (AWST' RuntimeEnvironment (ResourceT IO))) a
hoistDsl = (combineErrors . hoist unMyAwsM) 

streamEventsProducer :: QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent EventStore ()
streamEventsProducer direction streamId lastEvent batchSize =
  hoist hoistDsl $ Streams.streamEventsProducer direction streamId lastEvent batchSize

globalEventsProducer :: QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, RecordedEvent) EventStore ()
globalEventsProducer direction startPosition =
  hoist hoistDsl $ Streams.globalEventsProducer direction startPosition

readEvent :: StreamId -> Int64 -> EventStore (Maybe RecordedEvent)
readEvent streamId eventNumber =
  hoistDsl $ Streams.readEvent streamId eventNumber

writeEvent :: StreamId -> Maybe Int64 -> NonEmpty EventEntry -> EventStore Streams.EventWriteResult
writeEvent streamId ev eventEntries = hoistDsl $ Streams.writeEvent streamId ev eventEntries

combineErrors :: ExceptT
                   EventStoreActionError
                   (ExceptT
                      InterpreterError (AWST' RuntimeEnvironment (ResourceT IO)))
                   a
                 -> EventStore a 
combineErrors a = do
  r <- lift $ runExceptT (runExceptT a)
  case r of (Left e) -> throwError $  EventStoreErrorInterpreter e
            (Right (Left e)) -> throwError $  EventStoreErrorAction e
            (Right (Right result)) -> return result
