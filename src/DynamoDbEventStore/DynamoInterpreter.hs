{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DynamoDbEventStore.DynamoInterpreter where

import           Control.Exception
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

fieldStreamId = "streamId"
fieldEventNumber = "eventNumber"
fieldEventType = "eventType"
fieldBody = "body"
fieldPageKey = "pageKey"

getDynamoKeyForEvent :: EventKey -> PrimaryKey
getDynamoKeyForEvent (EventKey (StreamId streamId, evtNumber)) =
  hrk fieldStreamId (toValue streamId) fieldEventNumber (toValue evtNumber)

readText :: Read a => T.Text -> a
readText  = read . T.unpack

runCmd :: T.Text -> EventStoreCmd (IO a) -> IO a
runCmd _ (Wait' n) = n ()
runCmd tn (GetEvent' eventKey n) = do
  let key = getDynamoKeyForEvent eventKey
  let req0 = getItem tn key
  resp0 <- runCommand req0
  n $ getResult resp0
  where
    getResult :: GetItemResponse -> EventReadResult
    getResult r = do
      i <- girItem r
      eventTypeDvalue <- M.lookup fieldEventType i
      et <- fromValue eventTypeDvalue
      bodyDValue <- M.lookup fieldBody i
      b <- fromValue bodyDValue
      let pageKeyDValue = M.lookup fieldPageKey i >>= fromValue
      let pageKey = fmap readText $ pageKeyDValue
      return (et, b, pageKey)

runCmd tn (WriteEvent' (EventKey (StreamId streamId, evtNumber)) t d n) =
  catch writeItem exnHandler
    where
      -- todo: this function is not complete
      exnHandler (DdbError { ddbErrCode = ConditionalCheckFailedException }) = n EventExists
      writeItem = do
        let i = item [
                  attrAs text fieldStreamId streamId
                  , attrAs int fieldEventNumber (toInteger evtNumber)
                  , attrAs text fieldEventType t
                  , attr fieldBody d
                ]
        let conditions = Conditions CondAnd [ Condition fieldEventNumber IsNull ]
        let req0 = putItem tn i
        let req1 = req0 { piExpect = conditions }
        runCommand req1
        n WriteSuccess
runCmd tn (SetEventPage' eventKey pk n) =
  catch setEventPage exnHandler
    where
      -- todo: this function is not complete
      exnHandler (DdbError _ _ _ ) = n SetEventPageError
      setEventPage = do
        -- let conditions = Conditions CondAnd [ Condition fieldPageKey IsNull ]
        let key = getDynamoKeyForEvent eventKey
        let pageKeyAttribute = attrAs text fieldPageKey (T.pack $ show pk)
        let updatePageKey = au pageKeyAttribute
        let req0 = updateItem tn key [updatePageKey]
        -- let req1 = req0 { piExpect = conditions }
        runCommand req0
        n SetEventPageSuccess
runCmd _ (ScanUnpagedEvents' n) = error "todo"
runCmd _ (GetPageEntry' k n) = error "todo"
runCmd _ (WritePageEntry' k
           PageWriteRequest
           {
              expectedStatus = expectedStatus,
              newStatus = newStatus,
              newEntries = newEntries
           } n) =
  error "todo"

runTest :: T.Text -> EventStoreCmdM a -> IO a
runTest tableName = iterM $ runCmd tableName

evalProgram :: EventStoreCmdM a -> IO a
evalProgram program = do
  tableNameId :: Int <- getStdRandom (randomR (1,9999999999))
  let tableName = T.pack $ "testtable-" ++ show tableNameId
  let req0 = createTable tableName
        [AttributeDefinition fieldStreamId AttrString
         , AttributeDefinition fieldEventNumber AttrNumber]
        (HashAndRange fieldStreamId fieldEventNumber)
        (ProvisionedThroughput 1 1)
  resp0 <- runCommand req0
  runTest tableName program

runCommand r = do
    cfg <- Aws.baseConfiguration
    let cfg' = DdbConfiguration ddbLocal HTTP (Just 8000)
    Aws.simpleAws cfg cfg' r
