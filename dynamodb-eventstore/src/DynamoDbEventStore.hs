module DynamoDbEventStore
  (streamEventsProducer
  ,globalEventsProducer
  ,globalEventKeysProducer
  ,writeEvent
  ,readEvent
  ,buildTable
  ,doesTableExist
  ,runGlobalFeedWriter
  ,EventStoreError(..)
  ,EventStoreActionError(..)
  ,EventStore
  ,Streams.EventWriteResult(..)
  ,EventEntry(..)
  ,EventType(..)
  ,EventTime(..))
where

import DynamoDbEventStore.ProjectPrelude
import Control.Monad.State
import qualified DynamoDbEventStore.Streams as Streams
import DynamoDbEventStore.Types
    (RecordedEvent(..),QueryDirection,StreamId,EventStoreActionError,GlobalFeedPosition,EventKey)
import DynamoDbEventStore.AmazonkaImplementation (RuntimeEnvironment, InterpreterError, MyAwsM(..))
import qualified  DynamoDbEventStore.AmazonkaImplementation as AWS
import           DynamoDbEventStore.Storage.StreamItem (EventEntry(..),EventType(..),EventTime(..))
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import Control.Monad.Trans.AWS
import Control.Monad.Trans.Resource
import Control.Monad.Morph

data EventStoreError =
  EventStoreErrorInterpreter InterpreterError
  | EventStoreErrorAction EventStoreActionError
  deriving (Show, Eq)

type EventStore = ExceptT EventStoreError (AWST' RuntimeEnvironment (ResourceT IO))

hoistDsl
  :: (ExceptT EventStoreActionError MyAwsM) a -> (ExceptT EventStoreError (AWST' RuntimeEnvironment (ResourceT IO))) a
hoistDsl = combineErrors . hoist unMyAwsM 

buildTable :: Text -> EventStore ()
buildTable tableName = hoistDsl $ lift $ AWS.buildTable tableName

doesTableExist :: Text -> EventStore Bool
doesTableExist tableName = hoistDsl $ lift $ AWS.doesTableExist tableName

streamEventsProducer :: QueryDirection -> StreamId -> Maybe Int64 -> Natural -> Producer RecordedEvent EventStore ()
streamEventsProducer direction streamId lastEvent batchSize =
  hoist hoistDsl $ Streams.streamEventsProducer direction streamId lastEvent batchSize

globalEventsProducer :: QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, RecordedEvent) EventStore ()
globalEventsProducer direction startPosition =
  hoist hoistDsl $ Streams.globalEventsProducer direction startPosition

globalEventKeysProducer :: QueryDirection -> Maybe GlobalFeedPosition -> Producer (GlobalFeedPosition, EventKey) EventStore ()
globalEventKeysProducer direction startPosition =
  hoist hoistDsl $ Streams.globalEventKeysProducer direction startPosition

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

runGlobalFeedWriter :: EventStore ()
runGlobalFeedWriter =
  evalStateT (hoist hoistDsl GlobalFeedWriter.main) GlobalFeedWriter.emptyGlobalFeedWriterState
