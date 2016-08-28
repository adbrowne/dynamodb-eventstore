{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module DynamoDbEventStore.EventStoreCommands(
  StreamId(..),
  LogLevel(..),
  log',
  writeToDynamo',
  readFromDynamo',
  wait',
  scanNeedsPaging',
  queryTable',
  updateItem',
  setPulseStatus',
  readField,
  MonadEsDsl(..),
  MyAwsM(..),
  DynamoVersion,
  QueryDirection(..),
  RecordedEvent(..),
  EventKey(..),
  EventId(..),
  DynamoCmd(..),
  DynamoKey(..),
  DynamoWriteResult(..),
  DynamoReadResult(..),
  DynamoValues,
  ValueUpdate(..),
  PageKey(..),
  FeedEntry(..)
  ) where
import           BasicPrelude hiding (log)
import           Control.Lens              hiding ((.=))
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Concurrent.STM
import           Control.Concurrent.Async hiding (wait)
import           Control.Monad.Free.Church
import qualified DodgerBlue
import qualified DodgerBlue.Testing
import qualified DodgerBlue.IO             as DodgerIO
import           GHC.Natural

import           Data.Aeson
import           GHC.Generics

import qualified Data.HashMap.Strict       as HM
import qualified Data.Serialize            as Serialize
import           Data.Time.Clock
import qualified Data.UUID                 as UUID
import qualified Test.QuickCheck           as QC
import           TextShow.TH

import           DynamoDbEventStore.Types
import           DynamoDbEventStore.AmazonkaImplementation
import           Network.AWS.DynamoDB hiding (updateItem)
import           Network.AWS (MonadAWS(..))
import           Control.Monad.Trans.AWS hiding (LogLevel)
import           Control.Monad.Trans.Resource

newtype EventKey = EventKey (StreamId, Int64) deriving (Ord, Eq, Show)
deriveTextShow ''EventKey
data PageStatus = Version Int | Full | Verified deriving (Eq, Show, Generic)

newtype EventId = EventId { unEventId :: UUID.UUID } deriving (Show, Eq, Ord, Generic)

instance QC.Arbitrary PageKey where
  arbitrary =
    let
      positiveToPageKey (QC.Positive p) = PageKey p
    in positiveToPageKey <$> QC.arbitrary

instance Serialize.Serialize EventId where
  put (EventId uuid) = do
    let (w0, w1, w2, w3) = UUID.toWords uuid
    Serialize.put w0
    Serialize.put w1
    Serialize.put w2
    Serialize.put w3
  get = EventId <$> do
    w0 <- Serialize.get
    w1 <- Serialize.get
    w2 <- Serialize.get
    w3 <- Serialize.get
    return $ UUID.fromWords w0 w1 w2 w3

instance QC.Arbitrary EventId where
  arbitrary =
    EventId
      <$> (UUID.fromWords
            <$> QC.arbitrary
            <*> QC.arbitrary
            <*> QC.arbitrary
            <*> QC.arbitrary)

data RecordedEvent = RecordedEvent {
   recordedEventStreamId :: Text,
   recordedEventNumber   :: Int64,
   recordedEventData     :: ByteString,
   recordedEventType     :: Text,
   recordedEventCreated  :: UTCTime,
   recordedEventId       :: EventId,
   recordedEventIsJson   :: Bool
} deriving (Show, Eq, Ord)

instance ToJSON RecordedEvent where
  toJSON RecordedEvent{..} =
    object [ "streamId"    .= recordedEventStreamId
           , "eventNumber" .= recordedEventNumber
           , "eventData" .= decodeUtf8 recordedEventData
           , "eventType" .= recordedEventType
           ]

instance FromJSON PageStatus
instance ToJSON PageStatus

instance FromJSON EventKey where
  parseJSON (Object v) =
    EventKey <$>
    ((,) <$> v .: "streamId"
         <*> v .: "eventNumber")
  parseJSON _ = mzero
instance ToJSON EventKey where
  toJSON (EventKey(streamId, eventNumber)) =
    object [ "streamId"    .= streamId
           , "eventNumber" .= eventNumber
           ]

data DynamoCmd next =
  ReadFromDynamo' DynamoKey (Maybe DynamoReadResult -> next)
  | WriteToDynamo' DynamoKey DynamoValues DynamoVersion (DynamoWriteResult -> next)
  | QueryTable' QueryDirection Text Natural (Maybe Int64) ([DynamoReadResult] -> next)
  | UpdateItem' DynamoKey (HashMap Text ValueUpdate) (Bool -> next)
  | ScanNeedsPaging' ([DynamoKey] -> next)
  | Wait' Int next
  | SetPulseStatus' Bool next
  | Log' LogLevel Text next

deriving instance Functor DynamoCmd

class Monad m => MonadEsDsl m  where
  type QueueType m :: * -> *
  type MonadBase m :: * -> *
  newQueue :: forall a. Typeable a => m (QueueType m a)
  writeQueue :: forall a. Typeable a => QueueType m a -> a -> m ()
  readQueue :: forall a. Typeable a => QueueType m a -> m a
  tryReadQueue :: forall a. Typeable a => QueueType m a -> m (Maybe a)
  readFromDynamo :: DynamoKey -> m (Maybe DynamoReadResult)
  writeToDynamo :: DynamoKey -> DynamoValues -> DynamoVersion -> m (DynamoWriteResult)
  queryTable :: QueryDirection -> Text -> Natural -> Maybe Int64 -> m [DynamoReadResult]
  updateItem :: DynamoKey -> (HashMap Text ValueUpdate) -> m Bool
  log :: LogLevel -> Text -> m ()
  scanNeedsPaging :: m [DynamoKey]
  wait :: Int -> m ()
  forkChild :: MonadBase m () -> m ()
  setPulseStatus :: Bool -> m ()

wait' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => Int -> m ()
wait' seconds = DodgerBlue.Testing.customCmd $ Wait' seconds ()

setPulseStatus' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => Bool -> m ()
setPulseStatus' active = DodgerBlue.Testing.customCmd $ SetPulseStatus' active ()

log' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => LogLevel -> Text -> m ()
log' level message = DodgerBlue.Testing.customCmd $ Log' level message ()

scanNeedsPaging' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => m ([DynamoKey])
scanNeedsPaging' = DodgerBlue.Testing.customCmd $ ScanNeedsPaging' id

updateItem' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> (HashMap Text ValueUpdate) -> m (Bool)
updateItem' key updates = DodgerBlue.Testing.customCmd $ UpdateItem' key updates id

readFromDynamo' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> m (Maybe DynamoReadResult)
readFromDynamo' key = DodgerBlue.Testing.customCmd $ ReadFromDynamo' key id

writeToDynamo' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => DynamoKey -> DynamoValues -> DynamoVersion -> m (DynamoWriteResult)
writeToDynamo' key values version = DodgerBlue.Testing.customCmd $ WriteToDynamo' key values version id

queryTable' :: (MonadFree (DodgerBlue.CustomDsl q DynamoCmd) m) => QueryDirection -> Text -> Natural -> Maybe Int64 -> m ([DynamoReadResult])
queryTable' direction hashKey maxEvents startEvent = DodgerBlue.Testing.customCmd $ QueryTable' direction hashKey maxEvents startEvent id

instance MonadEsDsl (F (DodgerBlue.CustomDsl q DynamoCmd)) where
  type QueueType (F (DodgerBlue.CustomDsl q DynamoCmd)) = q
  type MonadBase (F (DodgerBlue.CustomDsl q DynamoCmd)) = (F (DodgerBlue.CustomDsl q DynamoCmd))
  newQueue = DodgerBlue.newQueue
  writeQueue = DodgerBlue.writeQueue
  readQueue = DodgerBlue.readQueue
  tryReadQueue = DodgerBlue.tryReadQueue
  forkChild = DodgerBlue.forkChild
  readFromDynamo = readFromDynamo'
  writeToDynamo = writeToDynamo'
  updateItem = updateItem'
  queryTable = queryTable'
  log = log'
  scanNeedsPaging = scanNeedsPaging'
  wait = wait'
  setPulseStatus = setPulseStatus'

--type MyAwsStack = ((ExceptT InterpreterError) (AWST' RuntimeEnvironment (ResourceT IO)))

forkChildIO :: MyAwsM () -> MyAwsM ()
forkChildIO (MyAwsM c) = MyAwsM $ do
  runtimeEnv <- ask
  _ <- lift $ allocate (async (runResourceT $ runAWST runtimeEnv (runExceptT c))) cancel
  return ()

instance MonadEsDsl MyAwsM where
  type QueueType MyAwsM = TQueue
  type MonadBase MyAwsM = MyAwsM
  newQueue = MyAwsM $ DodgerIO.newQueue
  writeQueue q a = MyAwsM $ DodgerIO.writeQueue q a
  readQueue = MyAwsM . DodgerIO.readQueue
  tryReadQueue = MyAwsM . DodgerIO.tryReadQueue
  forkChild c = forkChildIO c
  readFromDynamo = readFromDynamoAws
  writeToDynamo = writeToDynamoAws
  updateItem = updateItemAws
  queryTable = queryTableAws
  log = logAws
  scanNeedsPaging = scanNeedsPagingAws
  wait = waitAws
  setPulseStatus = setPulseStatusAws

instance MonadEsDsl m => MonadEsDsl (StateT s m) where
  type QueueType (StateT s m) = QueueType m
  type MonadBase (StateT s m) = MonadBase m
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
  forkChild = lift . forkChild
  readFromDynamo = lift . readFromDynamo
  writeToDynamo a b c = lift $ writeToDynamo a b c
  updateItem a b = lift $ updateItem a b
  queryTable a b c d = lift $ queryTable a b c d
  log a b = lift $ log a b
  scanNeedsPaging = scanNeedsPagingAws
  wait = lift . wait
  setPulseStatus = lift . setPulseStatus

instance MonadEsDsl m => MonadEsDsl (ExceptT e m) where
  type QueueType (ExceptT e m) = QueueType m
  type MonadBase (ExceptT e m) = MonadBase m
  newQueue = lift newQueue
  writeQueue q a = lift $ writeQueue q a
  readQueue = lift . readQueue
  tryReadQueue = lift . tryReadQueue
  forkChild = lift . forkChild
  readFromDynamo = lift . readFromDynamo
  writeToDynamo a b c = lift $ writeToDynamo a b c
  updateItem a b = lift $ updateItem a b
  queryTable a b c d = lift $ queryTable a b c d
  log a b = lift $ log a b
  scanNeedsPaging = scanNeedsPagingAws
  wait = lift . wait
  setPulseStatus = lift . setPulseStatus

readField :: (MonadError e m) => (Text -> e) -> Text -> Lens' AttributeValue (Maybe a) -> DynamoValues -> m a
readField = readFieldGeneric
