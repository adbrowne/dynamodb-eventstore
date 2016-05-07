{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           BasicPrelude
import           System.Exit
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

runDynamoLocal :: Env -> MyAwsStack a -> IO (Either Text a)
runDynamoLocal env x = do
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  runResourceT $ runAWST env $ reconfigure dynamo $ runExceptT x

runDynamoCloud :: Env -> MyAwsStack a -> IO (Either Text a)
runDynamoCloud env x = runResourceT $ runAWST env $ runExceptT x

runMyAws :: (MyAwsStack a -> ExceptT Text IO a) -> Text -> DynamoCmdM a -> ExceptT Text IO a
runMyAws runner tableName program = 
  runner $ runProgram tableName program

printEvent :: EventStoreAction -> IO EventStoreAction
printEvent a = do
  print . show $ a
  return a

runEventStoreAction :: (forall a. DynamoCmdM a -> ExceptT Text IO a) -> EventStoreAction -> IO (Either Text EventStoreResponse)
runEventStoreAction runner (PostEvent req) = do
  let program = postEventRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  return $ fmap (\x -> EventStoreResponse { eventStoreResponseToText = show x }) a
runEventStoreAction runner (ReadStream req) = do
  let program = getReadStreamRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  return $ fmap (\x -> EventStoreResponse { eventStoreResponseToText = show x }) a
runEventStoreAction runner (ReadAll req) = do
  let program = getReadAllRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  return $ fmap (\x -> EventStoreResponse { eventStoreResponseToText = show x }) a
runEventStoreAction runner (ReadEvent req) = do
  let program = getReadEventRequestProgram req
  a <- liftIO $ runExceptT $ runner program
  return $ fmap (\x -> EventStoreResponse { eventStoreResponseToText = show x }) a
runEventStoreAction _ (SubscribeAll _) = error "SubscribeAll not implemented" 

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

toExceptT :: forall a.( MyAwsStack a -> IO (Either Text a)) -> (MyAwsStack a -> ExceptT Text IO a)
toExceptT runner a = do
  result <- liftIO $ runner a
  case result of Left s -> throwError s
                 Right r -> return r

start :: Config -> ExceptT Text IO ()
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
   runApp :: (forall a. MyAwsStack a -> ExceptT Text IO a) -> Text -> ExceptT Text IO ()
   runApp runner tableName = do
     let runner' = runMyAws runner tableName
     exitMVar <- lift newEmptyMVar
     _ <- lift $ forkIO $ do
       result <- runExceptT $ runner' GlobalFeedWriter.main
       putMVar exitMVar result
     let httpPort = (configPort parsedConfig)
     let warpSettings = setPort httpPort $ setHost (fromString httpHost) defaultSettings
     putStrLn $ "Server listenting on: http://" <> fromString httpHost <> ":" <> show httpPort
     _ <- lift $ forkIO $ void $ scottyApp (app (printEvent >=> runEventStoreAction runner')) >>= runSettings warpSettings
     programResult <- liftIO $ takeMVar exitMVar
     print programResult
     case programResult of (Left err)         -> throwError err
                           (Right (Left err)) -> throwError err
                           _                  -> return ()
   failNoTable = putStrLn "Table does not exist"

checkForFailureOnExit :: ExceptT Text IO () -> IO ()
checkForFailureOnExit a = do
  result <- runExceptT a
  case result of Left m -> do
                             putStrLn $ "Error: " <> m
                             exitWith $ ExitFailure 1
                 Right () -> return ()
main :: IO ()
main = Opt.execParser opts >>= checkForFailureOnExit . start
  where
    opts = info (Opt.helper <*> config)
      ( fullDesc
     <> Opt.progDesc "DynamoDB event store"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
