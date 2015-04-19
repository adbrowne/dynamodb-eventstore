{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DynamoDbEventStore.DynamoInterpreter where

import           Control.Monad.Free
import           Data.Map                (Map)
import qualified Data.Map                as M
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text.Lazy          as TL
import qualified Data.Text               as T
import           System.Random
import           EventStoreActions
import           EventStoreCommands
import           Aws
import           Aws.Core
import           Aws.DynamoDb.Commands
import           Aws.DynamoDb.Core
--import           Control.Concurrent
--import           Control.Monad
--import           Control.Monad.Catch
--import           Data.Conduit
--import qualified Data.Conduit.List     as C
--import qualified Data.Text             as T
--import           Network.HTTP.Conduit  (withManager)

runCmd :: EventStoreCmd (IO a) -> IO a
runCmd (Wait' n) = n ()
runCmd (GetEvent' k f) = error "todo"
runCmd (WriteEvent' k t v n) = error "todo"
runCmd (SetEventPage' k pk n) = error "todo"
runCmd (ScanUnpagedEvents' n) = error "todo"
runCmd (GetPageEntry' k n) = error "todo"
runCmd (WritePageEntry' k PageWriteRequest { expectedStatus = expectedStatus, newStatus = newStatus, newEntries = newEntries } n) = error "todo"

runTest :: EventStoreCmdM a -> IO a
runTest = iterM runCmd

evalProgram :: EventStoreCmdM a -> IO a
evalProgram program = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  let req0 = createTable tableName
        [AttributeDefinition "name" AttrString]
        (HashOnly "name")
        (ProvisionedThroughput 1 1)
  resp0 <- runCommand req0
  runTest program

runCommand r = do
    cfg <- Aws.baseConfiguration
    let cfg' = DdbConfiguration ddbLocal HTTP (Just 8000)
    Aws.simpleAws cfg cfg' r
