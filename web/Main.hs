{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           BasicPrelude
import           Control.Lens
import           System.Exit
import           Control.Monad.Except
import           Network.Wai.Handler.Warp
import           Web.Scotty
import           System.IO (stdout)
import           DynamoDbEventStore.Webserver (app, realRunner, EventStoreActionRunner(..))
import           Control.Concurrent
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.GlobalFeedWriter (EventStoreActionError(..))
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           Options.Applicative as Opt
import qualified Data.Text               as T
import           Control.Monad.Trans.AWS
import qualified Control.Monad.Trans.AWS as AWS
import           Network.AWS.DynamoDB

runDynamoLocal :: Env -> MyAwsStack a -> IO (Either InterpreterError a)
runDynamoLocal env x = do
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  runResourceT $ runAWST env $ reconfigure dynamo $ runExceptT x

runDynamoCloud :: Env -> MyAwsStack a -> IO (Either InterpreterError a)
runDynamoCloud env x = runResourceT $ runAWST env $ runExceptT x

runMyAws :: (MyAwsStack a -> ExceptT InterpreterError IO a) -> Text -> DynamoCmdM a -> ExceptT InterpreterError IO a
runMyAws runner tableName program = 
  runner $ runProgram tableName program

printEvent :: (MonadIO m) => EventStoreAction -> m EventStoreAction
printEvent a = do
  liftIO $ print . show $ a
  return a

buildActionRunner :: (forall a. DynamoCmdM a -> ExceptT InterpreterError IO a) -> EventStoreActionRunner
buildActionRunner runner =
  EventStoreActionRunner { 
    eventStoreActionRunnerPostEvent = (\req -> liftIO $ runExceptT $ (PostEventResult <$> runner (postEventRequestProgram req)))
    , eventStoreActionRunnerReadEvent = (\req -> liftIO $ runExceptT $ (ReadEventResult <$> runner (getReadEventRequestProgram req)))
    , eventStoreActionRunnerReadStream = (\req -> liftIO $ runExceptT $ (ReadStreamResult <$> runner (getReadStreamRequestProgram req)))
    , eventStoreActionRunnerReadAll = (\req -> liftIO $ runExceptT $ (ReadAllResult <$> runner (getReadAllRequestProgram req)))
  }

data Config = Config 
  { configTableName :: String,
    configPort :: Int,
    configLocalDynamoDB :: Bool,
    configCreateTable :: Bool }

config :: Parser Config
config = Config
   <$> strOption (long "tableName"
       <> metavar "TABLENAME"
       <> help "DynamoDB table name")
   <*> option auto (long "port"
       <> value 2113
       <> short 'p'
       <> metavar "PORT"
       <> help "HTTP port")
   <*> flag False True
       (long "dynamoLocal" <> help "Use dynamodb local")
   <*> flag False True
       (long "createTable" <> short 'c' <> help "Create table if it does not exist")

httpHost :: String
httpHost = "127.0.0.1"

toExceptT :: forall a. (MyAwsStack a -> IO (Either InterpreterError a)) -> (MyAwsStack a -> ExceptT InterpreterError IO a)
toExceptT runner a = do
  result <- liftIO $ runner a
  case result of Left s -> throwError $ s
                 Right r -> return r

toApplicationError :: forall a. (MyAwsStack a -> IO (Either InterpreterError a)) -> (MyAwsStack a -> ExceptT ApplicationError IO a)
toApplicationError runner a = do
  result <- liftIO $ runner a
  case result of Left s -> throwError . ApplicationErrorInterpreter $ s
                 Right r -> return r

data ApplicationError =
  ApplicationErrorInterpreter InterpreterError |
  ApplicationErrorGlobalFeedWriter EventStoreActionError 
  deriving Show

start :: Config -> ExceptT ApplicationError IO ()
start parsedConfig = do
  let tableName = (T.pack . configTableName) parsedConfig
  logger <- liftIO $ newLogger AWS.Error stdout
  env <- (set envLogger logger) <$> newEnv Sydney Discover
  let interperter = (if configLocalDynamoDB parsedConfig then runDynamoLocal else runDynamoCloud) env
  let runner = toExceptT interperter
  tableAlreadyExists <- (toApplicationError interperter) $ doesTableExist tableName
  let shouldCreateTable = configCreateTable parsedConfig
  when (not tableAlreadyExists && shouldCreateTable) 
    (putStrLn "Creating table..." >> (toApplicationError interperter) (buildTable tableName) >> putStrLn "Table created")
  if tableAlreadyExists || shouldCreateTable then runApp runner tableName else failNoTable
  where
   runApp :: (forall a. MyAwsStack a -> ExceptT InterpreterError IO a) -> Text -> ExceptT ApplicationError IO ()
   runApp runner tableName = do
     let runner' = runMyAws runner tableName
     exitMVar <- lift newEmptyMVar
     _ <- lift $ forkIO $ do
       result <- runExceptT $ runner' GlobalFeedWriter.main
       putMVar exitMVar result
     let httpPort = (configPort parsedConfig)
     let warpSettings = setPort httpPort $ setHost (fromString httpHost) defaultSettings
     putStrLn $ "Server listenting on: http://" <> fromString httpHost <> ":" <> show httpPort
     _ <- lift $ forkIO $ void $ scottyApp (app (printEvent >=> realRunner (buildActionRunner runner'))) >>= runSettings warpSettings
     programResult <- liftIO $ takeMVar exitMVar
     print programResult
     case programResult of (Left err)         -> throwError $ ApplicationErrorInterpreter err
                           (Right (Left err)) -> throwError $ ApplicationErrorGlobalFeedWriter err
                           _                  -> return ()
   failNoTable = putStrLn "Table does not exist"

checkForFailureOnExit :: ExceptT ApplicationError IO () -> IO ()
checkForFailureOnExit a = do
  result <- runExceptT a
  case result of Left m -> do
                             putStrLn $ "Error: " <> show m
                             exitWith $ ExitFailure 1
                 Right () -> return ()
main :: IO ()
main = Opt.execParser opts >>= checkForFailureOnExit . start
  where
    opts = info (Opt.helper <*> config)
      ( fullDesc
     <> Opt.progDesc "DynamoDB event store"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
