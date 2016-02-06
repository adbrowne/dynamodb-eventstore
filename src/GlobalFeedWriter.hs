{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module GlobalFeedWriter (main) where

import           Control.Monad
import qualified Data.Text             as T
import qualified Data.HashMap.Lazy as HM
import qualified DynamoDbEventStore.Constants as Constants
import           EventStoreCommands

markKeyDone :: DynamoKey -> DynamoCmdM ()
markKeyDone key = do
  (entry :: Maybe DynamoReadResult) <- readFromDynamo' key
  _ <- maybe (return ()) removePagingKey entry
  return ()
  where
    removePagingKey :: DynamoReadResult -> DynamoCmdM ()
    removePagingKey DynamoReadResult { dynamoReadResultVersion = version, dynamoReadResultValue = value } = 
      let 
        value' = HM.delete Constants.needsPagingKey value
      in void (writeToDynamo' key value' (Just $ version + 1)) -- todo handle failure 

main :: DynamoCmdM ()
main = do
  scanResult <- scanNeedsPaging'
  forM_ scanResult markKeyDone
  log' Debug $ (T.pack . show . length) scanResult
  main
