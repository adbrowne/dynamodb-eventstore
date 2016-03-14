{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import qualified Data.Text.Lazy as TL
import           Web.Scotty
import           Webserver      (app)
import           Control.Monad.IO.Class (liftIO)
import           Control.Concurrent
import           DynamoDbEventStore.AmazonkaInterpreterNew
import           EventStoreActions
import qualified GlobalFeedWriter
import           System.Random
import qualified Data.Text               as T

showEvent :: T.Text -> EventStoreAction -> ActionM ()
showEvent tableName (PostEvent req) = do
  let program = postEventRequestProgramNew req
  a <- liftIO $ runProgram tableName program
  (html . TL.pack . show) a
showEvent tableName (ReadStream req) = do
  let program = getReadStreamRequestProgramNew req
  a <- liftIO $ runProgram tableName program
  json a
showEvent tableName (ReadAll req) = do
  let program = getReadAllRequestProgramNew req
  a <- liftIO $ runProgram tableName program
  json a
showEvent _ a = 
  (html . TL.pack . show) a

main :: IO ()
main = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  print tableName
  buildTable tableName
  _ <- forkIO $ runProgram tableName (GlobalFeedWriter.main)
  scotty 3000 (app (showEvent tableName))
