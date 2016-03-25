{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import qualified Data.Text.Lazy as TL
import           Web.Scotty
import           DynamoDbEventStore.Webserver (app)
import           Control.Monad.IO.Class (liftIO, MonadIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Catch (MonadThrow)
import           Control.Monad (when)
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

showEvent :: (forall a. DynamoCmdM a -> IO a) -> EventStoreAction -> ActionM ()
showEvent run (PostEvent req) = do
  let program = postEventRequestProgram req
  a <- liftIO $ run program
  (html . TL.pack . show) a
showEvent run (ReadStream req) = do
  let program = getReadStreamRequestProgram req
  a <- liftIO $ run program
  json a
showEvent run (ReadAll req) = do
  let program = getReadAllRequestProgram req
  a <- liftIO $ run program
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
  _ <- when (configCreateTable parsedConfig) (runner $ buildTable tableName)
  let runner' = runMyAws runner tableName
  _ <- forkIO $ runner' GlobalFeedWriter.main
  scotty 3000 (app (showEvent runner'))

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = info (Opt.helper <*> config)
      ( fullDesc
     <> Opt.progDesc "DynamoDB event store"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
