{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

module DynamoDbEventStore.GlobalFeedWriter (main, FeedEntry(FeedEntry), feedEntryStream, feedEntryNumber, feedEntryCount, dynamoWriteWithRetry, entryEventCount) where

import           BasicPrelude
import           Safe
import           Control.Monad.Except
import qualified Data.Sequence         as Seq
import qualified Data.Text             as T
import qualified Data.ByteString.Lazy  as BL
import qualified Data.ByteString       as BS
import qualified Data.HashMap.Lazy as HM
import qualified DynamoDbEventStore.Constants as Constants
import           DynamoDbEventStore.EventStoreCommands
import           Control.Lens
import           Network.AWS.DynamoDB
import qualified Data.Aeson as Aeson
import           Text.Printf (printf)
import qualified Test.QuickCheck as QC

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
    parseJSON _                = mempty

instance Aeson.ToJSON FeedEntry where
    toJSON (FeedEntry stream number entryCount) =
        Aeson.object ["s" Aeson..= stream, "n" Aeson..= number, "c" Aeson..=entryCount]

data FeedPage = FeedPage {
  feedPageNumber     :: Int,
  feedPageEntries    :: Seq.Seq FeedEntry,
  feedPageIsVerified :: Bool,
  feedPageVersion    :: Int
}

dynamoWriteWithRetry :: DynamoKey -> DynamoValues -> Int -> GlobalFeedWriterStack DynamoWriteResult
dynamoWriteWithRetry key value version = loop 0 DynamoWriteFailure
  where
    loop :: Int -> DynamoWriteResult -> GlobalFeedWriterStack DynamoWriteResult
    loop 100 previousResult = return previousResult
    loop count DynamoWriteFailure = (lift $ writeToDynamo' key value version) >>= loop (count  + 1)
    loop _ previousResult = return previousResult

getPageDynamoKey :: Int -> DynamoKey
getPageDynamoKey pageNumber =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

getMostRecentPage :: Int -> GlobalFeedWriterStack (Maybe FeedPage)
getMostRecentPage startPageNumber = 
  readFeedPage startPageNumber >>= findPage
  where
    readFeedPage :: Int -> GlobalFeedWriterStack (Maybe FeedPage)
    readFeedPage pageNumber = do
      dynamoEntry <- lift (readFromDynamo' $ getPageDynamoKey pageNumber)
      return $ toFeedPage startPageNumber <$> dynamoEntry
    toFeedPage :: Int -> DynamoReadResult -> FeedPage
    toFeedPage pageNumber readResult =
      let 
        pageValues = dynamoReadResultValue readResult
        isVerified = HM.member Constants.pageIsVerifiedKey pageValues
        version = dynamoReadResultVersion readResult
        entries = readPageBody pageValues
      in FeedPage { feedPageNumber = pageNumber, feedPageEntries = entries, feedPageIsVerified = isVerified, feedPageVersion = version }
    findPage :: Maybe FeedPage -> GlobalFeedWriterStack (Maybe FeedPage)
    findPage Nothing = return Nothing
    findPage (Just lastPage) = do
      let nextPage = feedPageNumber lastPage + 1
      dynamoEntry <- readFromDynamo' $ getPageDynamoKey nextPage
      let feedPage = toFeedPage nextPage <$> dynamoEntry
      case feedPage of Just _  -> findPage feedPage
                       Nothing -> return (Just lastPage)

entryIsPaged :: DynamoReadResult -> Bool
entryIsPaged dynamoItem =
  dynamoItem &
    dynamoReadResultValue &
    HM.member Constants.needsPagingKey &
    not

entryEventCount :: (MonadError Text m) => DynamoReadResult -> m Int
entryEventCount dynamoItem = 
  let 
    value = dynamoItem &
              dynamoReadResultValue &
              view (ix Constants.eventCountKey . avN) 
    parsedValue = value >>= (Safe.readMay . T.unpack)
  in case parsedValue of Nothing  -> throwError $ "Unable to parse eventEntryCount: " <> show value
                         (Just x) -> return x 

readPageBody :: DynamoValues -> Seq.Seq FeedEntry
readPageBody values = -- todo don't ignore errors
  fromMaybe Seq.empty $ view (ix Constants.pageBodyKey . avB) values >>= Aeson.decodeStrict

nextVersion :: DynamoReadResult -> Int
nextVersion readResult = dynamoReadResultVersion readResult + 1

toDynamoKey :: StreamId -> Int64 -> DynamoKey
toDynamoKey (StreamId streamId) = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId)

setPageEntryPageNumber :: Int -> FeedEntry -> GlobalFeedWriterStack ()
setPageEntryPageNumber pageNumber feedEntry = do
  let streamId = feedEntryStream feedEntry
  let dynamoKey = toDynamoKey streamId  (feedEntryNumber feedEntry)
  eventEntry <- readFromDynamoMustExist dynamoKey
  let newValue = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (stringAttributeValue (show pageNumber)) . dynamoReadResultValue) eventEntry
  void $ dynamoWriteWithRetry dynamoKey newValue (nextVersion eventEntry)

stringAttributeValue :: Text -> AttributeValue
stringAttributeValue t = set avS (Just t) attributeValue

verifyPage :: Int -> GlobalFeedWriterStack ()
verifyPage (-1)       = return ()
verifyPage pageNumber = do
  let pageDynamoKey = getPageDynamoKey pageNumber
  page <- readFromDynamoMustExist pageDynamoKey
  let pageValues = dynamoReadResultValue page
  let pageVersion = dynamoReadResultVersion page
  log' Debug ("verifyPage " <> show pageNumber <> " go value " <> show pageValues)
  unless (HM.member Constants.pageIsVerifiedKey pageValues) $ do
    let entries = readPageBody pageValues
    log' Debug ("setPageEntry for " <> show entries)
    void $ traverse (setPageEntryPageNumber pageNumber) entries
    let newValues = HM.insert Constants.pageIsVerifiedKey (stringAttributeValue "Verified") pageValues
    void $ dynamoWriteWithRetry pageDynamoKey newValues (pageVersion + 1)

logIf :: Bool -> LogLevel -> Text -> GlobalFeedWriterStack ()
logIf True logLevel t = lift $ log' logLevel t
logIf False _ _ = return ()

readFromDynamoMustExist :: DynamoKey -> GlobalFeedWriterStack DynamoReadResult
readFromDynamoMustExist key = do
  r <- readFromDynamo' key
  case r of Just x -> return x
            Nothing -> throwError $ "Could not find item: " <> show key

emptyFeedPage :: Int -> FeedPage 
emptyFeedPage pageNumber = FeedPage { feedPageNumber = pageNumber, feedPageEntries = Seq.empty, feedPageIsVerified = False, feedPageVersion = -1 }

getCurrentPage :: GlobalFeedWriterStack FeedPage
getCurrentPage = do
  mostRecentPage <- getMostRecentPage 0
  let mostRecentPageNumber = maybe (-1) feedPageNumber mostRecentPage
  let startNewPage = maybe True (\page -> (length . feedPageEntries) page >= 10) mostRecentPage
  if startNewPage then do
    verifyPage mostRecentPageNumber
    return $ emptyFeedPage (mostRecentPageNumber + 1)
  else
    return $ fromMaybe (emptyFeedPage (mostRecentPageNumber + 1)) mostRecentPage

setEventEntryPage :: DynamoKey -> Int -> GlobalFeedWriterStack DynamoWriteResult
setEventEntryPage key pageNumber = do
    eventEntry <- readFromDynamoMustExist key
    let values = dynamoReadResultValue eventEntry
    let version = dynamoReadResultVersion eventEntry
    let values' = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (set avS (Just (show pageNumber)) attributeValue)) values
    dynamoWriteWithRetry key values' (version + 1)

itemToJsonByteString :: Aeson.ToJSON a => a -> BS.ByteString
itemToJsonByteString = BL.toStrict . Aeson.encode . Aeson.toJSON

getPreviousEntryEventNumber :: DynamoKey -> GlobalFeedWriterStack Int64
getPreviousEntryEventNumber (DynamoKey _streamId (0)) = return (-1)
getPreviousEntryEventNumber (DynamoKey streamId eventNumber) = do
  result <- lift $ queryBackward' streamId 1 (Just $ eventNumber)
  return $ getEventNumber result
  where 
    getEventNumber [] = error "Could not find previous event"
    getEventNumber ((DynamoReadResult (DynamoKey _key en) _version _values):_) = en

feedEntriesContainsEntry :: StreamId -> Int64 -> Seq.Seq FeedEntry -> Bool
feedEntriesContainsEntry streamId eventNumber = any (\(FeedEntry sId evN _) -> sId == streamId && evN == eventNumber)

updateGlobalFeed :: DynamoKey -> GlobalFeedWriterStack ()
updateGlobalFeed itemKey@DynamoKey { dynamoKeyKey = itemHashKey, dynamoKeyEventNumber = itemEventNumber } = do
  lift $ log' Debug ("updateGlobalFeed " <> show itemKey)
  let streamId = StreamId $ T.drop (T.length Constants.streamDynamoKeyPrefix) itemHashKey
  currentPage <- getCurrentPage
  item <- readFromDynamoMustExist itemKey
  let itemIsPaged = entryIsPaged item
  logIf itemIsPaged Debug ("itemIsPaged" <> show itemKey)
  unless itemIsPaged $ do
    let currentPageFeedEntries = feedPageEntries currentPage
    let pageNumber = feedPageNumber currentPage
    if feedEntriesContainsEntry streamId itemEventNumber currentPageFeedEntries then
      void $ setEventEntryPage itemKey pageNumber
    else do
      itemEventCount <- entryEventCount item
      let feedEntry = itemToJsonByteString (currentPageFeedEntries |> FeedEntry streamId itemEventNumber itemEventCount)
      previousEntryEventNumber <- getPreviousEntryEventNumber itemKey
      log' Debug $ "itemKey: " <> show itemKey <> " previousEntryEventNumber: " <> show previousEntryEventNumber
      when (previousEntryEventNumber > -1) (updateGlobalFeed itemKey { dynamoKeyEventNumber = previousEntryEventNumber })
      let version = feedPageVersion currentPage + 1
      pageResult <- dynamoWriteWithRetry (getPageDynamoKey (feedPageNumber currentPage)) (HM.singleton Constants.pageBodyKey (set avB (Just feedEntry) attributeValue)) version
      onPageResult pageNumber pageResult
      return ()
  return ()
  where
    onPageResult :: Int -> DynamoWriteResult -> GlobalFeedWriterStack ()
    onPageResult _ DynamoWriteWrongVersion = do
      log' Debug "Got wrong version writing page"
      updateGlobalFeed itemKey
    onPageResult pageNumber DynamoWriteSuccess = 
      void $ setEventEntryPage itemKey pageNumber
    onPageResult pageNumber DynamoWriteFailure = throwError ("DynamoWriteFailure on writing page: " <> show pageNumber)

type GlobalFeedWriterStack = ExceptT Text DynamoCmdM

runLoop :: GlobalFeedWriterStack ()
runLoop = do
  scanResult <- lift scanNeedsPaging'
  forM_ scanResult updateGlobalFeed
  when (null scanResult) (wait' 1000)
  setPulseStatus' $ case scanResult of [] -> False
                                       _  -> True
main :: DynamoCmdM (Either Text ())
main = do
  result <- runExceptT runLoop
  case result of (Left errMsg) -> return $ Left errMsg
                 (Right ())    -> main
