{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module DynamoDbEventStore.InMemoryDynamoTable
  (InMemoryDynamoTable
  , emptyDynamoTable) where

import           BasicPrelude
import           Control.Lens
import           Control.Lens.TH
import qualified Data.HashMap.Strict                   as HM
import qualified Data.Map                              as Map
import           DynamoDbEventStore.EventStoreCommands

data InMemoryDynamoTable = InMemoryDynamoTable {
  _inMemoryDynamoTableTable :: HM.HashMap Text (Map Int64 (Int, DynamoValues))
                           }
  deriving (Show)

$(makeLenses ''InMemoryDynamoTable)

emptyDynamoTable :: InMemoryDynamoTable
emptyDynamoTable = InMemoryDynamoTable {
  _inMemoryDynamoTableTable = mempty }


read :: DynamoKey -> InMemoryDynamoTable -> Maybe DynamoReadResult
read key@DynamoKey{..} db =
 let
   entry = db ^.
     (inMemoryDynamoTableTable
      . at dynamoKeyKey
      . non mempty
      . at dynamoKeyEventNumber)
 in uncurry (DynamoReadResult key) <$> entry

write :: DynamoKey -> DynamoValues -> DynamoVersion -> InMemoryDynamoTable -> (DynamoWriteResult, InMemoryDynamoTable)
write DynamoKey{..} values version db =
  let
    entryLocation =
      inMemoryDynamoTableTable
      . at dynamoKeyKey . non mempty
      . at dynamoKeyEventNumber
    currentVersion = fst <$> db ^. entryLocation
  in writeVersion version currentVersion
  where
    writeVersion 0 Nothing = performWrite 0
    writeVersion _newVersion Nothing = (DynamoWriteWrongVersion, db)
    writeVersion newVersion (Just currentVersion)
      | currentVersion == newVersion - 1 = performWrite newVersion
      | otherwise = (DynamoWriteWrongVersion, db)
    performWrite newVersion =
      let
        newEntry = (newVersion, values)
        entryLocation =
          inMemoryDynamoTableTable
          . at dynamoKeyKey . non mempty
          . at dynamoKeyEventNumber
        db' = set entryLocation (Just newEntry) db
      in (DynamoWriteSuccess, db')
