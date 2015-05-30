{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import           Data.Text.Lazy (Text, pack)
import           Web.Scotty
import           Webserver      (app)
import           Control.Monad.IO.Class (liftIO)
import           DynamoDbEventStore.DynamoInterpreter
import           EventStoreCommands
import           EventStoreActions
import           System.Random
import qualified Data.Text               as T

showEvent tableName (PostEvent req) = do
  let program = postEventRequestProgram req
  a <- liftIO $ runProgram tableName program
  (html . pack . show) a
showEvent tableName a = do
  (html . pack . show) a

main = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  putStrLn $ show tableName
  buildTable tableName
  scotty 3000 (app (showEvent tableName))
