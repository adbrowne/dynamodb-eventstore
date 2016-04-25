{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DynamoDbEventStore.GlobalFeedWriter (main, FeedEntry(FeedEntry), feedEntryStream, feedEntryNumber, feedEntryCount, dynamoWriteWithRetry, entryEventCount) where

import           Safe
import           Control.Monad
import           Data.Int
import qualified Data.Sequence         as Seq
import qualified Data.Text             as T
import qualified Data.ByteString.Lazy  as BL
import qualified Data.ByteString       as BS
import qualified Data.HashMap.Lazy as HM
import qualified DynamoDbEventStore.Constants as Constants
import           DynamoDbEventStore.EventStoreCommands
import           Data.Maybe
import           Data.Monoid
import           Control.Lens
import           Network.AWS.DynamoDB
import qualified Data.Aeson as Aeson
import           Control.Applicative
import           Text.Printf (printf)
import qualified Test.QuickCheck as QC

toText :: Show s => s -> T.Text
toText = T.pack . show

data FeedEntry = FeedEntry {
  feedEntryStream :: StreamId,
  feedEntryNumber :: Int64,
  feedEntryCount :: Int
} deriving (Eq, Show)

instance QC.Arbitrary FeedEntry where
  arbitrary =
    FeedEntry <$> QC.arbitrary
              <*> QC.arbitrary
              <*> QC.arbitrary

instance Aeson.FromJSON FeedEntry where
    parseJSON (Aeson.Object v) = FeedEntry <$>
                           v Aeson..: "s" <*>
                           v Aeson..: "n" <*>
                           v Aeson..: "c"
    parseJSON _                = empty

instance Aeson.ToJSON FeedEntry where
    toJSON (FeedEntry stream number entryCount) =
        Aeson.object ["s" Aeson..= stream, "n" Aeson..= number, "c" Aeson..=entryCount]

data FeedPage = FeedPage {
  feedPageNumber     :: Int,
  feedPageEntries    :: Seq.Seq FeedEntry,
  feedPageIsVerified :: Bool,
  feedPageVersion    :: Int
}

dynamoWriteWithRetry :: DynamoKey -> DynamoValues -> Int -> DynamoCmdM DynamoWriteResult
dynamoWriteWithRetry key value version = loop 0 DynamoWriteFailure
  where
    loop :: Int -> DynamoWriteResult -> DynamoCmdM DynamoWriteResult
    loop 100 previousResult = return previousResult
    loop count DynamoWriteFailure = writeToDynamo' key value version >>= loop (count  + 1)
    loop _ previousResult = return previousResult

getPageDynamoKey :: Int -> DynamoKey
getPageDynamoKey pageNumber =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

getMostRecentPage :: Int -> DynamoCmdM (Maybe FeedPage)
getMostRecentPage startPageNumber = 
  readFeedPage startPageNumber >>= findPage
  where
    readFeedPage :: Int -> DynamoCmdM (Maybe FeedPage)
    readFeedPage pageNumber = do
      dynamoEntry <- readFromDynamo' $ getPageDynamoKey pageNumber
      return $ toFeedPage startPageNumber <$> dynamoEntry
    toFeedPage :: Int -> DynamoReadResult -> FeedPage
    toFeedPage pageNumber readResult =
      let 
        pageValues = dynamoReadResultValue readResult
        isVerified = HM.member Constants.pageIsVerifiedKey pageValues
        version = dynamoReadResultVersion readResult
        entries = readPageBody pageValues
      in FeedPage { feedPageNumber = pageNumber, feedPageEntries = entries, feedPageIsVerified = isVerified, feedPageVersion = version }
    findPage :: Maybe FeedPage -> DynamoCmdM (Maybe FeedPage)
    findPage Nothing = return Nothing
    findPage (Just lastPage) = do
      let nextPage = feedPageNumber lastPage + 1
      dynamoEntry <- readFromDynamo' $ getPageDynamoKey nextPage
      let feedPage = toFeedPage nextPage <$> dynamoEntry
      case feedPage of Just _  -> findPage feedPage
                       Nothing -> return (Just lastPage)

entryIsPaged :: DynamoReadResult -> Bool
entryIsPaged dynamoItem = do
  dynamoItem &
    dynamoReadResultValue &
    HM.member Constants.needsPagingKey &
    not

entryEventCount :: DynamoReadResult -> Int
entryEventCount dynamoItem = 
  let 
    value = dynamoItem &
              dynamoReadResultValue &
              view (ix Constants.eventCountKey . avN) 
  in fromJust $ value >>= (Safe.readMay . T.unpack)

readPageBody :: DynamoValues -> Seq.Seq FeedEntry
readPageBody values = -- todo don't ignore errors
  fromMaybe Seq.empty $ view (ix Constants.pageBodyKey . avB) values >>= Aeson.decodeStrict

nextVersion :: DynamoReadResult -> Int
nextVersion readResult = dynamoReadResultVersion readResult + 1

toDynamoKey :: StreamId -> Int64 -> DynamoKey
toDynamoKey (StreamId streamId) = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId)

setPageEntryPageNumber :: Int -> FeedEntry -> DynamoCmdM ()
setPageEntryPageNumber pageNumber feedEntry = do
  let streamId = feedEntryStream feedEntry
  let dynamoKey = toDynamoKey streamId  (feedEntryNumber feedEntry)
  eventEntry <- readFromDynamoMustExist dynamoKey
  let newValue = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (stringAttributeValue (toText pageNumber)) . dynamoReadResultValue) eventEntry
  void $ dynamoWriteWithRetry dynamoKey newValue (nextVersion eventEntry)

stringAttributeValue :: T.Text -> AttributeValue
stringAttributeValue t = set avS (Just t) attributeValue

verifyPage :: Int -> DynamoCmdM ()
verifyPage (-1)       = return ()
verifyPage pageNumber = do
  let pageDynamoKey = getPageDynamoKey pageNumber
  page <- readFromDynamoMustExist pageDynamoKey
  let pageValues = dynamoReadResultValue page
  let pageVersion = dynamoReadResultVersion page
  log' Debug ("verifyPage " <> toText pageNumber <> " go value " <> toText pageValues)
  unless (HM.member Constants.pageIsVerifiedKey pageValues) $ do
    let entries = readPageBody pageValues
    log' Debug ("setPageEntry for " <> toText entries)
    void $ traverse (setPageEntryPageNumber pageNumber) entries
    let newValues = HM.insert Constants.pageIsVerifiedKey (stringAttributeValue "Verified") pageValues
    void $ dynamoWriteWithRetry pageDynamoKey newValues (pageVersion + 1)

logIf :: Bool -> LogLevel -> T.Text -> DynamoCmdM ()
logIf True logLevel t = log' logLevel t
logIf False _ _ = return ()

readFromDynamoMustExist :: DynamoKey -> DynamoCmdM DynamoReadResult
readFromDynamoMustExist key = do
  r <- readFromDynamo' key
  case r of Just x -> return x
            Nothing -> fatalError' ("Could not find item: " <> toText key)

emptyFeedPage :: Int -> FeedPage 
emptyFeedPage pageNumber = FeedPage { feedPageNumber = pageNumber, feedPageEntries = Seq.empty, feedPageIsVerified = False, feedPageVersion = -1 }

getCurrentPage :: DynamoCmdM FeedPage
getCurrentPage = do
  mostRecentPage <- getMostRecentPage 0
  let mostRecentPageNumber = maybe (-1) feedPageNumber mostRecentPage
  let startNewPage = maybe True (\page -> (length . feedPageEntries) page >= 10) mostRecentPage
  if startNewPage then do
    verifyPage mostRecentPageNumber
    return $ emptyFeedPage (mostRecentPageNumber + 1)
  else
    return $ fromMaybe (emptyFeedPage (mostRecentPageNumber + 1)) mostRecentPage

setEventEntryPage :: DynamoKey -> Int -> DynamoCmdM DynamoWriteResult
setEventEntryPage key pageNumber = do
    eventEntry <- readFromDynamoMustExist key
    let values = dynamoReadResultValue eventEntry
    let version = dynamoReadResultVersion eventEntry
    let values' = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (set avS (Just (toText pageNumber)) attributeValue)) values
    dynamoWriteWithRetry key values' (version + 1)

itemToJsonByteString :: Aeson.ToJSON a => a -> BS.ByteString
itemToJsonByteString = BL.toStrict . Aeson.encode . Aeson.toJSON

getPreviousEntryEventNumber :: DynamoKey -> DynamoCmdM Int64
getPreviousEntryEventNumber (DynamoKey _streamId (0)) = return (-1)
getPreviousEntryEventNumber (DynamoKey streamId eventNumber) = do
  result <- queryBackward' streamId 1 (Just $ eventNumber - 1)
  return $ getEventNumber result
  where 
    getEventNumber [] = error "Could not find previous event"
    getEventNumber ((DynamoReadResult (DynamoKey _key en) _version _values):_) = en

updateGlobalFeed :: DynamoKey -> DynamoCmdM ()
updateGlobalFeed itemKey@DynamoKey { dynamoKeyKey = itemHashKey, dynamoKeyEventNumber = itemEventNumber } = do
  log' Debug ("updateGlobalFeed " <> toText itemKey)
  let streamId = StreamId $ T.drop (T.length Constants.streamDynamoKeyPrefix) itemHashKey
  currentPage <- getCurrentPage
  item <- readFromDynamoMustExist itemKey
  let itemIsPaged = entryIsPaged item
  logIf itemIsPaged Debug ("itemIsPaged" <> toText itemKey)
  unless itemIsPaged $ do
    let itemEventCount = entryEventCount item
    let feedEntry = itemToJsonByteString (feedPageEntries currentPage |> FeedEntry streamId itemEventNumber itemEventCount)
    previousEntryEventNumber <- getPreviousEntryEventNumber itemKey
    when (previousEntryEventNumber > -1) (updateGlobalFeed itemKey { dynamoKeyEventNumber = previousEntryEventNumber })
    let version = feedPageVersion currentPage + 1
    pageResult <- dynamoWriteWithRetry (getPageDynamoKey (feedPageNumber currentPage)) (HM.singleton Constants.pageBodyKey (set avB (Just feedEntry) attributeValue)) version
    onPageResult (feedPageNumber currentPage) pageResult
    return ()
  return ()
  where
    onPageResult :: Int -> DynamoWriteResult -> DynamoCmdM ()
    onPageResult _ DynamoWriteWrongVersion = do
      log' Debug "Got wrong version writing page"
      updateGlobalFeed itemKey
    onPageResult pageNumber DynamoWriteSuccess = 
      void $ setEventEntryPage itemKey pageNumber
    onPageResult pageNumber DynamoWriteFailure = fatalError' ("DynamoWriteFailure on writing page: " <> toText pageNumber)

main :: DynamoCmdM ()
main = forever $ do
  scanResult <- scanNeedsPaging'
  forM_ scanResult updateGlobalFeed
  when (null scanResult) (wait' 1000)
  setPulseStatus' $ case scanResult of [] -> False
                                       _  -> True
