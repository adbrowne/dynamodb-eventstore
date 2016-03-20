{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
module Main where

import qualified Data.Text.Lazy as TL
import           Web.Scotty
import           DynamoDbEventStore.Webserver (app)
import           Control.Monad.IO.Class (liftIO)
import           Control.Concurrent
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import qualified DynamoDbEventStore.GlobalFeedWriter as GlobalFeedWriter
import           System.Random
import qualified Data.Text               as T
import Network.AWS

runMyAws :: T.Text -> DynamoCmdM a -> IO a
runMyAws tableName program = do
  e <- newEnv Sydney Discover
  runResourceT $ runAWS e $ runProgram tableName program

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

main :: IO ()
main = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  print tableName
  buildTable tableName
  _ <- forkIO $ runMyAws tableName GlobalFeedWriter.main
  scotty 3000 (app (showEvent (runMyAws tableName)))
