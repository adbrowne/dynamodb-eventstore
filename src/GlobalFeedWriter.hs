{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module GlobalFeedWriter (main) where

import           Control.Monad
import qualified Data.Text             as T
import qualified Data.HashMap.Lazy as HM
import qualified DynamoDbEventStore.Constants as Constants
import           EventStoreCommands

toText :: Show s => s -> T.Text
toText = T.pack . show

dynamoWriteWithRetry :: DynamoKey -> DynamoValues -> Maybe Int -> DynamoCmdM DynamoWriteResult
dynamoWriteWithRetry key value version = loop 0 DynamoWriteFailure
  where 
    loop :: Int -> DynamoWriteResult -> DynamoCmdM DynamoWriteResult
    loop 100 previousResult = return previousResult
    loop count DynamoWriteFailure = writeToDynamo' key value version >>= loop (count  + 1)
    loop _ previousResult = return previousResult

markKeyDone :: DynamoKey -> DynamoCmdM ()
markKeyDone key = do
  (entry :: Maybe DynamoReadResult) <- readFromDynamo' key
  _ <- maybe (return ()) removePagingKey entry
  return ()
  where
    removePagingKey :: DynamoReadResult -> DynamoCmdM ()
    removePagingKey DynamoReadResult { dynamoReadResultVersion = version, dynamoReadResultValue = value } = do
      let 
        value' = HM.delete Constants.needsPagingKey value
        version' = Just $ version + 1
      result <- dynamoWriteWithRetry key value' version'
      case result of DynamoWriteSuccess -> return ()
                     DynamoWriteWrongVersion -> markKeyDone key
                     DynamoWriteFailure -> fatalError' "Too many failures writing to dynamo"

main :: DynamoCmdM ()
main = loop 0
  where 
  loop :: Int -> DynamoCmdM ()
  loop idleCount = do
    setPulseStatus' idleCount
    scanResult <- scanNeedsPaging'
    forM_ scanResult markKeyDone
    log' Debug $ (toText . length) scanResult
    let idleCount' = case scanResult of [] -> idleCount + 1
                                        _  -> 0

    loop idleCount'
