{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module DynamoDbEventStore.InMemoryDynamoTable
  (InMemoryDynamoTable
  , emptyDynamoTable
  , readDb
  , writeDb
  , queryDb
  , scanNeedsPagingDb
  ) where

import           BasicPrelude
import           Control.Lens
import qualified Data.HashMap.Strict                   as HM
import qualified Data.Map.Strict                       as Map
import qualified Data.Set                              as Set
import qualified DynamoDbEventStore.Constants          as Constants
import           DynamoDbEventStore.EventStoreCommands
import           GHC.Natural

data InMemoryDynamoTable = InMemoryDynamoTable {
  _inMemoryDynamoTableTable         :: HM.HashMap Text (Map Int64 (Int, DynamoValues))
  , _inMemoryDynamoTableNeedsPaging :: Set DynamoKey }
  deriving (Eq, Show)

$(makeLenses ''InMemoryDynamoTable)

emptyDynamoTable :: InMemoryDynamoTable
emptyDynamoTable = InMemoryDynamoTable {
  _inMemoryDynamoTableTable = mempty
  , _inMemoryDynamoTableNeedsPaging = mempty}

readDb :: DynamoKey -> InMemoryDynamoTable -> Maybe DynamoReadResult
readDb key@DynamoKey{..} db =
 let
   entry = db ^.
     (inMemoryDynamoTableTable
      . at dynamoKeyKey
      . non mempty
      . at dynamoKeyEventNumber)
   buildReadResult (version, value) = DynamoReadResult {
    dynamoReadResultKey = key
    , dynamoReadResultVersion = version
    , dynamoReadResultValue = value }
 in buildReadResult <$> entry

writeDb :: DynamoKey -> DynamoValues -> DynamoVersion -> InMemoryDynamoTable -> (DynamoWriteResult, InMemoryDynamoTable)
writeDb key@DynamoKey{..} values version db =
  let
    entryLocation =
      inMemoryDynamoTableTable
      . at dynamoKeyKey . non mempty
      . at dynamoKeyEventNumber
    currentVersion = fst <$> db ^. entryLocation
  in writeVersion version currentVersion
  where
    entryNeedsPaging = HM.member Constants.needsPagingKey values
    writeVersion 0 Nothing = performWrite 0
    writeVersion _newVersion Nothing = (DynamoWriteWrongVersion, db)
    writeVersion newVersion (Just currentVersion)
      | currentVersion == newVersion - 1 = performWrite newVersion
      | otherwise = (DynamoWriteWrongVersion, db)
    updatePagingTable =
      if entryNeedsPaging then
        Set.insert key
      else
        Set.delete key
    performWrite newVersion =
      let
        newEntry = (newVersion, values)
        entryLocation =
          inMemoryDynamoTableTable
          . at dynamoKeyKey . non mempty
          . at dynamoKeyEventNumber
        db' = set entryLocation (Just newEntry) db
                & over inMemoryDynamoTableNeedsPaging updatePagingTable
      in (DynamoWriteSuccess, db')

queryDb :: QueryDirection -> Text -> Natural -> Maybe Int64 -> InMemoryDynamoTable -> [DynamoReadResult]
queryDb direction streamId maxEvents startEvent db =
  let
    rangeItems = db ^. inMemoryDynamoTableTable . at streamId . non mempty
    items = case (direction, startEvent) of
      (QueryDirectionForward, Nothing) -> Map.toAscList rangeItems
      (QueryDirectionForward, Just startEventNumber) ->
        rangeItems
        & Map.split startEventNumber
        & snd
        & Map.toAscList
      (QueryDirectionBackward, Nothing) -> Map.toDescList rangeItems
      (QueryDirectionBackward, Just startEventNumber) ->
        rangeItems
        & Map.split startEventNumber
        & fst
        & Map.toDescList
    itemsCutOff = take (fromIntegral maxEvents) items
    toReadResult (eventNumber, (currentVersion, value)) = DynamoReadResult {
      dynamoReadResultKey = DynamoKey streamId eventNumber
      , dynamoReadResultVersion = currentVersion
      , dynamoReadResultValue = value }
  in toReadResult <$> itemsCutOff

scanNeedsPagingDb :: InMemoryDynamoTable -> [DynamoKey]
scanNeedsPagingDb = view $ inMemoryDynamoTableNeedsPaging . to Set.toAscList
