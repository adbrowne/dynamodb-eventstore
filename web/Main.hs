{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           BasicPrelude
import qualified Data.Text.Lazy as TL
import           Network.Wai.Handler.Warp
import           Web.Scotty
import           DynamoDbEventStore.Webserver (app)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Catch (MonadThrow)
import           Control.Concurrent
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           Options.Applicative as Opt
import qualified Data.Text               as T
import           Network.AWS
import           Network.AWS.DynamoDB

runDynamoLocal :: (MonadThrow m, MonadIO m, MonadBaseControl IO m, HasEnv r) => r -> AWS a -> m a
runDynamoLocal env x = do
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  runResourceT $ runAWS env $ reconfigure dynamo x

runDynamoCloud :: (MonadThrow m, MonadIO m, MonadBaseControl IO m, HasEnv r) => r -> AWS a -> m a
runDynamoCloud env x = runResourceT $ runAWS env x

runMyAws :: (AWS a -> IO a) -> Text -> DynamoCmdM a -> IO a
runMyAws runner tableName program = 
  runner $ runProgram tableName program

printEvent :: EventStoreAction -> ActionM EventStoreAction
printEvent a = do
  liftIO . print . show $ a
  return a

runEventStoreAction :: (forall a. DynamoCmdM a -> IO a) -> EventStoreAction -> ActionM ()
runEventStoreAction runner (PostEvent req) = do
  let program = postEventRequestProgram req
  a <- liftIO $ runner program
  (html . TL.fromStrict . show) a
runEventStoreAction runner (ReadStream req) = do
  let program = getReadStreamRequestProgram req
  a <- liftIO $ runner program
  json a
runEventStoreAction runner (ReadAll req) = do
  let program = getReadAllRequestProgram req
  a <- liftIO $ runner program
  json a
runEventStoreAction runner (ReadEvent req) = do
  let program = getReadEventRequestProgram req
  a <- liftIO $ runner program
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

start :: Config -> IO ()
start parsedConfig = do
  let tableName = (T.pack . configTableName) parsedConfig
  env <- newEnv Sydney Discover
  let runner = (if configLocalDynamoDB parsedConfig then runDynamoLocal else runDynamoCloud) env
  tableAlreadyExists <- runner $ doesTableExist tableName
  let shouldCreateTable = configCreateTable parsedConfig
  when (not tableAlreadyExists && shouldCreateTable) 
    (putStrLn "Creating table..." >> runner (buildTable tableName) >> putStrLn "Table created")
  if tableAlreadyExists || shouldCreateTable then runApp runner tableName else failNoTable
  where
   runApp :: (forall a. AWS a -> IO a) -> Text -> IO ()
   runApp runner tableName = do
     let runner' = runMyAws runner tableName
     _ <- forkIO $ runner' GlobalFeedWriter.main
     let warpSettings = setPort httpPort $ setHost (fromString httpHost) defaultSettings
     putStrLn $ "Server listenting on: http://" <> fromString httpHost <> ":" <> show httpPort
     scottyApp (app (printEvent >=> runEventStoreAction runner')) >>= runSettings warpSettings
     return ()
   failNoTable = putStrLn "Table does not exist"

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = info (Opt.helper <*> config)
      ( fullDesc
     <> Opt.progDesc "DynamoDB event store"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
