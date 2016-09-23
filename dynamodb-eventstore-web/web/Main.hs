{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import BasicPrelude
import Control.Concurrent
import Control.Monad.Except
import Control.Monad.State
import Control.Monad.Trans.AWS
import qualified Data.Text as T
import DynamoDbEventStore.AmazonkaImplementation
import DynamoDbEventStore.EventStoreActions
import DynamoDbEventStore.EventStoreCommands
import DynamoDbEventStore.GlobalFeedWriter
       (EventStoreActionError(..))
import qualified DynamoDbEventStore.GlobalFeedWriter
       as GlobalFeedWriter
import DynamoDbEventStore.Webserver
       (EventStoreActionRunner(..), app, realRunner)
import Network.Wai.Handler.Warp
import Options.Applicative as Opt
import System.Exit
import System.Metrics hiding (Value)
import qualified System.Metrics.Counter as Counter
import qualified System.Metrics.Distribution as Distribution
import System.Remote.Monitoring
import Web.Scotty

printEvent
    :: (MonadIO m)
    => EventStoreAction -> m EventStoreAction
printEvent a = do
    liftIO $ print . tshow $ a
    return a

buildActionRunner
    :: MonadEsDsl m
    => (forall a. m a -> ExceptT InterpreterError IO a)
    -> EventStoreActionRunner
buildActionRunner runner = 
    EventStoreActionRunner
    { eventStoreActionRunnerPostEvent = (\req -> 
                                              liftIO $
                                              runExceptT $
                                              (PostEventResult <$>
                                               runWithState
                                                   (postEventRequestProgram req)))
    , eventStoreActionRunnerReadEvent = (\req -> 
                                              liftIO $
                                              runExceptT $
                                              (ReadEventResult <$>
                                               runWithState
                                                   (getReadEventRequestProgram
                                                        req)))
    , eventStoreActionRunnerReadStream = (\req -> 
                                               liftIO $
                                               runExceptT $
                                               (ReadStreamResult <$>
                                                runWithState
                                                    (getReadStreamRequestProgram
                                                         req)))
    , eventStoreActionRunnerReadAll = (\req -> 
                                            liftIO $
                                            runExceptT $
                                            (ReadAllResult <$>
                                             runWithState
                                                 (getReadAllRequestProgram req)))
    }
  where
    runWithState p = 
        runner $
        runExceptT $ evalStateT p GlobalFeedWriter.emptyGlobalFeedWriterState

data Config = Config
    { configTableName :: String
    , configPort :: Int
    , configLocalDynamoDB :: Bool
    , configCreateTable :: Bool
    } 

config :: Parser Config
config = 
    Config <$>
    strOption
        (long "tableName" <> metavar "TABLENAME" <> help "DynamoDB table name") <*>
    option
        auto
        (long "port" <> value 2113 <> short 'p' <> metavar "PORT" <>
         help "HTTP port") <*>
    flag False True (long "dynamoLocal" <> help "Use dynamodb local") <*>
    flag
        False
        True
        (long "createTable" <> short 'c' <>
         help "Create table if it does not exist")

httpHost :: String
httpHost = "127.0.0.1"

toExceptT' :: IO (Either e a) -> ExceptT e (IO) a
toExceptT' p = do
    a <- lift p
    case a of
        Left s -> throwError $ s
        Right r -> return r

toApplicationError
    :: forall a. 
       (MyAwsM a -> IO (Either InterpreterError a))
    -> (MyAwsM a -> ExceptT ApplicationError IO a)
toApplicationError runner a = do
    result <- liftIO $ runner a
    case result of
        Left s -> throwError . ApplicationErrorInterpreter $ s
        Right r -> return r

data ApplicationError
    = ApplicationErrorInterpreter InterpreterError
    | ApplicationErrorGlobalFeedWriter EventStoreActionError
    deriving ((Show))

forkAndSupervise :: Text -> IO () -> IO ()
forkAndSupervise processName = void . forkIO . handle onError
  where
    onError :: SomeException -> IO ()
    onError e = do
        putStrLn $ "Exception in " <> processName
        putStrLn . T.pack $ displayException e
        threadDelay 10000000 -- 10 seconds

printError
    :: (Show a)
    => a -> IO ()
printError err = putStrLn $ "Error: " <> tshow err

forkGlobalFeedWriter :: (forall a. MyAwsM a -> ExceptT InterpreterError IO a)
                     -> IO ()
forkGlobalFeedWriter runner = 
    forkAndSupervise "GlobalFeedWriter" $
    do pageKeyPositionCache <- newCacheAws 10
       result <- 
           runExceptT $
           runner $
           runExceptT $
           evalStateT
               (GlobalFeedWriter.main pageKeyPositionCache)
               GlobalFeedWriter.emptyGlobalFeedWriterState
       case result of
           (Left err) -> printError (ApplicationErrorInterpreter err)
           (Right (Left err)) -> 
               printError (ApplicationErrorGlobalFeedWriter err)
           _ -> return ()

startWebServer
    :: MonadEsDsl m
    => (forall a. m a -> ExceptT InterpreterError IO a) -> Config -> IO ()
startWebServer runner parsedConfig = do
    let httpPort = configPort parsedConfig
    let warpSettings = 
            setPort httpPort $ setHost (fromString httpHost) defaultSettings
    let baseUri = "http://" <> fromString httpHost <> ":" <> tshow httpPort
    putStrLn $ "Server listenting on: " <> baseUri
    void $
        scottyApp
            (app (printEvent >=> realRunner baseUri (buildActionRunner runner))) >>=
        runSettings warpSettings

startMetrics :: IO MetricLogs
startMetrics = do
    metricServer <- forkServer "localhost" 8001
    let store = serverMetricStore metricServer
    readItemPair <- createPair store "readItem"
    writeItemPair <- createPair store "writeItem"
    updateItemPair <- createPair store "updateItem"
    queryPair <- createPair store "query"
    scanPair <- createPair store "scan"
    return
        MetricLogs
        { metricLogsReadItem = readItemPair
        , metricLogsWriteItem = writeItemPair
        , metricLogsUpdateItem = updateItemPair
        , metricLogsQuery = queryPair
        , metricLogsScan = scanPair
        , metricLogsStore = store
        }
  where
    createPair store name = do
        theCounter <- createCounter ("dynamodb-eventstore." <> name) store
        theDistribution <- 
            createDistribution ("dynamodb-eventstore." <> name <> "_ms") store
        return $
            MetricLogsPair
                (Counter.inc theCounter)
                (Distribution.add theDistribution)

start :: Config -> ExceptT ApplicationError IO ()
start parsedConfig = do
    let tableName = (T.pack . configTableName) parsedConfig
    metrics <- liftIO startMetrics
    --logger <- liftIO $ newLogger AWS.Error stdout
    ---awsEnv <- set envLogger logger <$> newEnv Sydney Discover
    awsEnv <- newEnv Sydney Discover
    let interperter = 
            (if configLocalDynamoDB parsedConfig
                 then runDynamoLocal
                 else runDynamoCloud)
    let runtimeEnvironment = 
            RuntimeEnvironment
            { _runtimeEnvironmentMetricLogs = metrics
            , _runtimeEnvironmentAmazonkaEnv = awsEnv
            , _runtimeEnvironmentTableName = tableName
            }
    let runner p = toExceptT' $ interperter runtimeEnvironment p
    tableAlreadyExists <- 
        toApplicationError (interperter runtimeEnvironment) $
        doesTableExist tableName
    let shouldCreateTable = configCreateTable parsedConfig
    when
        (not tableAlreadyExists && shouldCreateTable)
        (putStrLn "Creating table..." >>
         toApplicationError
             (interperter runtimeEnvironment)
             (buildTable tableName) >>
         putStrLn "Table created")
    if tableAlreadyExists || shouldCreateTable
        then runApp runner tableName
        else failNoTable
  where
    runApp
        :: (forall a. MyAwsM a -> ExceptT InterpreterError IO a)
        -> Text
        -> ExceptT ApplicationError IO ()
    runApp runner _tableName
    --let runner' = runMyAws runner tableName
     = do
        liftIO $ forkGlobalFeedWriter runner
        liftIO $ startWebServer runner parsedConfig
    failNoTable = putStrLn "Table does not exist"

checkForFailureOnExit :: ExceptT ApplicationError IO () -> IO ()
checkForFailureOnExit a = do
    result <- runExceptT a
    case result of
        Left m -> do
            printError m
            exitWith $ ExitFailure 1
        Right () -> return ()

main :: IO ()
main = Opt.execParser opts >>= checkForFailureOnExit . start
  where
    opts = 
        info
            (Opt.helper <*> config)
            (fullDesc <> Opt.progDesc "DynamoDB event store" <>
             Opt.header
                 "DynamoDB Event Store - all your events are belong to us")