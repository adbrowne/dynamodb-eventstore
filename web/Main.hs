{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import qualified Data.Text.Lazy as TL
import           Network.Wai.Handler.Warp
import           Web.Scotty
import           DynamoDbEventStore.Webserver (app)
import           Control.Monad.IO.Class (liftIO, MonadIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Catch (MonadThrow)
import           Control.Monad (when, (>=>))
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

runMyAws :: (AWS a -> IO a) -> T.Text -> DynamoCmdM a -> IO a
runMyAws runner tableName program = 
  runner $ runProgram tableName program

printEvent :: EventStoreAction -> ActionM EventStoreAction
printEvent a = do
  liftIO . print . show $ a
  return a

showEvent :: (forall a. DynamoCmdM a -> IO a) -> EventStoreAction -> ActionM ()
showEvent runner (PostEvent req) = do
  let program = postEventRequestProgram req
  a <- liftIO $ runner program
  (html . TL.pack . show) a
showEvent runner (ReadStream req) = do
  let program = getReadStreamRequestProgram req
  a <- liftIO $ runner program
  json a
showEvent runner (ReadAll req) = do
  let program = getReadAllRequestProgram req
  a <- liftIO $ runner program
  json a
showEvent _ a = 
  (html . TL.pack . show) a

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

start :: Config -> IO ()
start parsedConfig = do
  let tableName = (T.pack . configTableName) parsedConfig
  env <- newEnv Sydney Discover
  let runner = (if configLocalDynamoDB parsedConfig then runDynamoLocal else runDynamoCloud) env
  tableAlreadyExists <- runner $ doesTableExist tableName
  let shouldCreateTable = configCreateTable parsedConfig
  when (not tableAlreadyExists && shouldCreateTable) 
    (putStrLn "Creating table..." >> runner (buildTable tableName))
  if tableAlreadyExists || shouldCreateTable then runApp runner tableName else failNoTable
  where
   runApp :: (forall a. AWS a -> IO a) -> T.Text -> IO ()
   runApp runner tableName = do
     let runner' = runMyAws runner tableName
     _ <- forkIO $ runner' GlobalFeedWriter.main
     let warpSettings = setPort 2113 $ setHost "127.0.0.1" defaultSettings
     scottyApp (app (printEvent >=> showEvent runner')) >>= runSettings warpSettings
     return ()
   failNoTable = putStrLn "Table does not exist"

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = info (Opt.helper <*> config)
      ( fullDesc
     <> Opt.progDesc "DynamoDB event store"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
