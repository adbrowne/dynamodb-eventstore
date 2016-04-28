{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           BasicPrelude
import           System.Exit
import qualified Data.Text.Lazy as TL
import           Control.Monad.Except
import           Network.Wai.Handler.Warp
import           Web.Scotty
import           DynamoDbEventStore.Webserver (app)
import           Control.Concurrent
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           Options.Applicative as Opt
import qualified Data.Text               as T
import           Control.Monad.Trans.AWS
import           Network.AWS.DynamoDB

runDynamoLocal :: Env -> MyAwsStack a -> IO (Either String a)
runDynamoLocal env x = do
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  runResourceT $ runAWST env $ reconfigure dynamo $ runExceptT x

runDynamoCloud :: Env -> MyAwsStack a -> IO (Either String a)
runDynamoCloud env x = runResourceT $ runAWST env $ runExceptT x

runMyAws :: (MyAwsStack a -> ExceptT String IO a) -> Text -> DynamoCmdM a -> ExceptT String IO a
runMyAws runner tableName program = 
  runner $ runProgram tableName program

printEvent :: EventStoreAction -> ActionM EventStoreAction
printEvent a = do
  liftIO . print . show $ a
  return a

runEventStoreAction :: (forall a. DynamoCmdM a -> ExceptT String IO a) -> EventStoreAction -> ActionM ()
runEventStoreAction runner (PostEvent req) = do
  let program = postEventRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  (html . TL.fromStrict . show) a
runEventStoreAction runner (ReadStream req) = do
  let program = getReadStreamRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  json a
runEventStoreAction runner (ReadAll req) = do
  let program = getReadAllRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  json a
runEventStoreAction runner (ReadEvent req) = do
  let program = getReadEventRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  json a
runEventStoreAction _ (SubscribeAll _) = error "SubscribeAll not implemented" 

data Config = Config 
  { configTableName :: String,
    configLocalDynamoDB :: Bool,
    configCreateTable :: Bool }

config :: Parser Config
config = Config
   <$> strOption (long "tableName"
       <> metavar "TABLENAME"
       <> help "DynamoDB table name")
   <*> flag False True
       (long "dynamoLocal" <> help "Use dynamodb local")
   <*> flag False True
       (long "createTable" <> short 'c' <> help "Create table if it does not exist")

httpPort :: Int
httpPort = 2113

httpHost :: String
httpHost = "127.0.0.1"

toExceptT :: forall a.( MyAwsStack a -> IO (Either String a)) -> (MyAwsStack a -> ExceptT String IO a)
toExceptT runner a = do
  result <- liftIO $ runner a
  case result of Left s -> throwError s
                 Right r -> return r

start :: Config -> ExceptT String IO ()
start parsedConfig = do
  let tableName = (T.pack . configTableName) parsedConfig
  env <- newEnv Sydney Discover
  let runner = toExceptT $ (if configLocalDynamoDB parsedConfig then runDynamoLocal else runDynamoCloud) env
  tableAlreadyExists <- runner $ doesTableExist tableName
  let shouldCreateTable = configCreateTable parsedConfig
  when (not tableAlreadyExists && shouldCreateTable) 
    (putStrLn "Creating table..." >> runner (buildTable tableName) >> putStrLn "Table created")
  if tableAlreadyExists || shouldCreateTable then runApp runner tableName else failNoTable
  where
   runApp :: (forall a. MyAwsStack a -> ExceptT String IO a) -> Text -> ExceptT String IO ()
   runApp runner tableName = do
     let runner' = runMyAws runner tableName
     _ <- lift $ forkIO $ void $ runExceptT $ runner' GlobalFeedWriter.main
     let warpSettings = setPort httpPort $ setHost (fromString httpHost) defaultSettings
     putStrLn $ "Server listenting on: http://" <> fromString httpHost <> ":" <> show httpPort
     lift $ scottyApp (app (printEvent >=> runEventStoreAction runner')) >>= runSettings warpSettings
     return ()
   failNoTable = putStrLn "Table does not exist"

checkForFailureOnExit :: ExceptT String IO () -> IO ()
checkForFailureOnExit a = do
  result <- runExceptT a
  case result of Left m -> do
                             putStrLn $ "Error: " <> fromString m
                             exitWith $ ExitFailure 1
                 Right () -> return ()
main :: IO ()
main = Opt.execParser opts >>= checkForFailureOnExit . start
  where
    opts = info (Opt.helper <*> config)
      ( fullDesc
     <> Opt.progDesc "DynamoDB event store"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
