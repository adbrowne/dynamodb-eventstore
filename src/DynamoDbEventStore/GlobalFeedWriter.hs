{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module DynamoDbEventStore.GlobalFeedWriter (
  main,
  dynamoWriteWithRetry,
  entryEventCount,
  writePage,
  getPageDynamoKey,
  getLatestStoredPage,
  emptyGlobalFeedWriterState,
  DynamoCmdWithErrors,
  GlobalFeedPosition(..),
  EventStoreActionError(..)) where

import           BasicPrelude hiding (log)
import           Control.Lens
import           Control.Monad.Except
import           Control.Monad.State
import qualified Data.Aeson                            as Aeson
import qualified Data.ByteString                       as BS
import qualified Data.ByteString.Lazy                  as BL
import           Data.Foldable
import qualified Data.HashMap.Lazy                     as HM
import qualified Data.Sequence                         as Seq
import qualified Data.Set                              as Set
import qualified Data.Text                             as T
import qualified DynamoDbEventStore.Constants          as Constants
import           DynamoDbEventStore.EventStoreCommands 
import           Network.AWS.DynamoDB hiding (updateItem)
import           Safe
import qualified Test.QuickCheck                       as QC
import           Text.Printf                           (printf)

data EventStoreActionError =
  EventStoreActionErrorFieldMissing Text |
  EventStoreActionErrorCouldNotReadEventCount (Maybe Text) |
  EventStoreActionErrorJsonDecodeError String |
  EventStoreActionErrorBodyDecode DynamoKey String |
  EventStoreActionErrorEventDoesNotExist DynamoKey |
  EventStoreActionErrorOnWritingPage PageKey |
  EventstoreActionErrorCouldNotFindPreviousEntry DynamoKey |
  EventStoreActionErrorCouldNotFindEvent EventKey |
  EventStoreActionErrorInvalidGlobalFeedPosition GlobalFeedPosition |
  EventStoreActionErrorInvalidGlobalFeedPage PageKey |
  EventStoreActionErrorWriteFailure DynamoKey |
  EventStoreActionErrorUpdateFailure DynamoKey |
  EventStoreActionErrorHeadCurrentPageMissing |
  EventStoreActionErrorHeadCurrentPageFormat Text
  deriving (Show, Eq)

data GlobalFeedPosition = GlobalFeedPosition {
    globalFeedPositionPage   :: PageKey
  , globalFeedPositionOffset :: Int
} deriving (Show, Eq, Ord)

instance QC.Arbitrary GlobalFeedPosition where
  arbitrary = GlobalFeedPosition
                <$> QC.arbitrary
                <*> ((\(QC.Positive p) -> p) <$> QC.arbitrary)

data FeedPage = FeedPage {
  feedPageNumber     :: PageKey,
  feedPageEntries    :: Seq.Seq FeedEntry,
  feedPageIsVerified :: Bool,
  feedPageVersion    :: Int
}

loopUntilSuccess :: Monad m => Integer -> (a -> Bool) -> m a -> m a
loopUntilSuccess maxTries f action =
  action >>= loop (maxTries - 1)
  where
    loop 0 lastResult = return lastResult
    loop _ lastResult | f lastResult = return lastResult
    loop triesRemaining _ = action >>= loop (triesRemaining - 1)

dynamoWriteWithRetry :: DynamoCmdWithErrors q m => DynamoKey -> DynamoValues -> Int -> m DynamoWriteResult
dynamoWriteWithRetry key value version = do
  finalResult <- loopUntilSuccess 100 (/= DynamoWriteFailure) (writeToDynamo key value version)
  checkFinalResult finalResult
  where
    checkFinalResult DynamoWriteSuccess = return DynamoWriteSuccess
    checkFinalResult DynamoWriteWrongVersion = return DynamoWriteWrongVersion
    checkFinalResult DynamoWriteFailure = throwError $ EventStoreActionErrorWriteFailure key

headDynamoKey :: DynamoKey
headDynamoKey = DynamoKey { dynamoKeyKey = "$head", dynamoKeyEventNumber = 0 }

latestPageKey :: Text
latestPageKey = "latestPage"

data HeadData = HeadData {
  headDataCurrentPage :: PageKey,
  headDataVersion     :: Int }

readExcept :: (MonadError e m) => (Read a) => (Text -> e) -> Text -> m a
readExcept err t =
  let
    parsed = BasicPrelude.readMay t
  in case parsed of Nothing  -> throwError $ err t
                    (Just a) -> return a

type DynamoCmdWithErrors q m = (MonadEsDsl q m, MonadError EventStoreActionError m)

readHeadData :: DynamoCmdWithErrors q m => m HeadData
readHeadData = do
  currentHead <- readFromDynamo headDynamoKey
  readHead currentHead
  where
    readHead Nothing = return HeadData { headDataCurrentPage = 0, headDataVersion = 0 }
    readHead (Just DynamoReadResult{..}) = do
      currentPage <- PageKey <$> (readField (const EventStoreActionErrorHeadCurrentPageMissing) latestPageKey avN dynamoReadResultValue >>= readExcept EventStoreActionErrorHeadCurrentPageFormat)
      return HeadData {
        headDataCurrentPage = currentPage,
        headDataVersion = dynamoReadResultVersion }

getLatestStoredPage :: DynamoCmdWithErrors q m => m PageKey
getLatestStoredPage = headDataCurrentPage <$> readHeadData

trySetLatestPage :: DynamoCmdWithErrors q m => PageKey -> m ()
trySetLatestPage latestPage = do
  HeadData{..} <- readHeadData
  when (latestPage > headDataCurrentPage) $ do
    let value = HM.singleton latestPageKey  (set avN (Just . show $  latestPage) attributeValue)
    void (writeToDynamo headDynamoKey value (headDataVersion + 1))
  return ()

getPageDynamoKey :: PageKey -> DynamoKey
getPageDynamoKey (PageKey pageNumber) =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

getMostRecentPage :: GlobalFeedWriterStack q m => PageKey -> m (Maybe FeedPage)
getMostRecentPage startPageNumber =
  readFeedPage startPageNumber >>= findPage
  where
    readFeedPage :: GlobalFeedWriterStack q m => PageKey -> m (Maybe FeedPage)
    readFeedPage pageNumber = do
      dynamoEntry <- readFromDynamo $ getPageDynamoKey pageNumber
      return $ toFeedPage startPageNumber <$> dynamoEntry
    toFeedPage :: PageKey -> DynamoReadResult -> FeedPage
    toFeedPage pageNumber readResult =
      let
        pageValues = dynamoReadResultValue readResult
        isVerified = HM.member Constants.pageIsVerifiedKey pageValues
        version = dynamoReadResultVersion readResult
        entries = readPageBody pageValues
      in FeedPage { feedPageNumber = pageNumber, feedPageEntries = entries, feedPageIsVerified = isVerified, feedPageVersion = version }
    findPage :: GlobalFeedWriterStack q m => Maybe FeedPage -> m (Maybe FeedPage)
    findPage Nothing = return Nothing
    findPage (Just lastPage) = do
      let nextPage = feedPageNumber lastPage + 1
      dynamoEntry <- readFromDynamo $ getPageDynamoKey nextPage
      let feedPage = toFeedPage nextPage <$> dynamoEntry
      case feedPage of Just _  -> findPage feedPage
                       Nothing -> return (Just lastPage)

entryIsPaged :: DynamoReadResult -> Bool
entryIsPaged dynamoItem =
  dynamoItem &
    dynamoReadResultValue &
    HM.member Constants.needsPagingKey &
    not

entryEventCount :: (MonadError EventStoreActionError m) => DynamoReadResult -> m Int
entryEventCount dynamoItem =
  let
    value = dynamoItem &
              dynamoReadResultValue &
              view (ix Constants.eventCountKey . avN)
    parsedValue = value >>= (Safe.readMay . T.unpack)
  in case parsedValue of Nothing  -> throwError $ EventStoreActionErrorCouldNotReadEventCount value
                         (Just x) -> return x

readPageBody :: DynamoValues -> Seq.Seq FeedEntry
readPageBody values = -- todo don't ignore errors
  fromMaybe Seq.empty $ view (ix Constants.pageBodyKey . avB) values >>= Aeson.decodeStrict

toDynamoKey :: StreamId -> Int64 -> DynamoKey
toDynamoKey (StreamId streamId) = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId)

eventKeyToDynamoKey :: EventKey -> DynamoKey
eventKeyToDynamoKey (EventKey(streamId, eventNumber)) = toDynamoKey streamId eventNumber

updateItemWithRetry :: (MonadEsDsl q m, MonadError EventStoreActionError m) => DynamoKey -> HashMap Text ValueUpdate -> m ()
updateItemWithRetry key updates = do
  result <- loopUntilSuccess 100 id (updateItem key updates)
  unless result (throwError $ EventStoreActionErrorUpdateFailure key)

setEventEntryPage :: GlobalFeedWriterStack q m => DynamoKey -> PageKey -> m ()
setEventEntryPage key (PageKey pageNumber) = do
    let updates =
          HM.fromList [
           (Constants.needsPagingKey, ValueUpdateDelete)
           , (Constants.eventPageNumberKey, ValueUpdateSet (set avS (Just (show pageNumber)) attributeValue))
                      ]
    updateItemWithRetry key updates

setPageEntryPageNumber :: GlobalFeedWriterStack q m => PageKey -> FeedEntry -> m ()
setPageEntryPageNumber pageNumber feedEntry = do
  let streamId = feedEntryStream feedEntry
  let dynamoKey = toDynamoKey streamId  (feedEntryNumber feedEntry)
  void $ setEventEntryPage dynamoKey pageNumber

stringAttributeValue :: Text -> AttributeValue
stringAttributeValue t = set avS (Just t) attributeValue

verifyPage :: GlobalFeedWriterStack q m => PageKey -> m ()
verifyPage (PageKey (-1))       = return ()
verifyPage pageNumber = do
  let pageDynamoKey = getPageDynamoKey pageNumber
  page <- readFromDynamoMustExist pageDynamoKey
  let pageValues = dynamoReadResultValue page
  let pageVersion = dynamoReadResultVersion page
  log Debug ("verifyPage " <> show pageNumber <> " go value " <> show pageValues)
  unless (HM.member Constants.pageIsVerifiedKey pageValues) $ do
    let entries = readPageBody pageValues
    log Debug ("setPageEntry for " <> show entries)
    void $ traverse (setPageEntryPageNumber pageNumber) entries
    let newValues = HM.insert Constants.pageIsVerifiedKey (stringAttributeValue "Verified") pageValues
    void (dynamoWriteWithRetry pageDynamoKey newValues (pageVersion + 1))

readFromDynamoMustExist :: GlobalFeedWriterStack q m => DynamoKey -> m DynamoReadResult
readFromDynamoMustExist key = do
  r <- readFromDynamo key
  case r of Just x -> return x
            Nothing -> throwError $ EventStoreActionErrorEventDoesNotExist key

emptyFeedPage :: PageKey -> FeedPage
emptyFeedPage pageNumber = FeedPage { feedPageNumber = pageNumber, feedPageEntries = Seq.empty, feedPageIsVerified = False, feedPageVersion = -1 }

pageSizeCutoff :: Int
pageSizeCutoff = 10

getCurrentPage :: GlobalFeedWriterStack q m => m FeedPage
getCurrentPage = do
  mostRecentKnownPage <- gets globalFeedWriterStateCurrentPage
  mostRecentPage <- getMostRecentPage =<< maybe (headDataCurrentPage <$> readHeadData) return mostRecentKnownPage
  modify (\s -> s { globalFeedWriterStateCurrentPage = feedPageNumber <$> mostRecentPage})
  let mostRecentPageNumber = maybe (PageKey (-1)) feedPageNumber mostRecentPage
  let startNewPage = maybe True (\page -> (length . feedPageEntries) page >= pageSizeCutoff) mostRecentPage
  if startNewPage then do
    verifyPage mostRecentPageNumber
    return $ emptyFeedPage (mostRecentPageNumber + 1)
  else
    return $ fromMaybe (emptyFeedPage (mostRecentPageNumber + 1)) mostRecentPage

itemToJsonByteString :: Aeson.ToJSON a => a -> BS.ByteString
itemToJsonByteString = BL.toStrict . Aeson.encode . Aeson.toJSON

readResultToDynamoKey :: DynamoReadResult -> DynamoKey
readResultToDynamoKey DynamoReadResult {..} = dynamoReadResultKey

readResultToEventKey :: DynamoReadResult -> EventKey
readResultToEventKey = dynamoKeyToEventKey . readResultToDynamoKey

writePage :: GlobalFeedWriterStack q m => PageKey -> Seq FeedEntry -> DynamoVersion -> m DynamoWriteResult
writePage pageNumber entries version = do
  let feedEntry = itemToJsonByteString entries
  let dynamoKey = getPageDynamoKey pageNumber
  let body = HM.singleton Constants.pageBodyKey (set avB (Just feedEntry) attributeValue)
  dynamoWriteWithRetry dynamoKey body version

readItems :: GlobalFeedWriterStack q m => [EventKey] -> m [DynamoReadResult]
readItems keys =
  sequence $ readFromDynamoMustExist . eventKeyToDynamoKey <$> keys

feedEntryToDynamoKey :: FeedEntry -> DynamoKey
feedEntryToDynamoKey = eventKeyToDynamoKey . feedEntryToEventKey

feedEntryToEventKey :: FeedEntry -> EventKey
feedEntryToEventKey FeedEntry{..} = EventKey (feedEntryStream, feedEntryNumber )

getUnpagedPreviousEntries :: GlobalFeedWriterStack q m => [EventKey] -> m [EventKey]
getUnpagedPreviousEntries keys =
  join <$> sequence (getPreviousEntryIfUnpaged <$> keys)
  where
    getPreviousEntryIfUnpaged :: GlobalFeedWriterStack q m => EventKey -> m [EventKey]
    getPreviousEntryIfUnpaged (EventKey (_, 0)) = return []
    getPreviousEntryIfUnpaged eventKey = do
      let key = eventKeyToDynamoKey eventKey
      let DynamoKey{..} = key
      result <- queryTable QueryDirectionBackward dynamoKeyKey 1 (Just dynamoKeyEventNumber)
      case result of [] -> throwError $ EventstoreActionErrorCouldNotFindPreviousEntry key
                     (x:_xs) -> if entryIsPaged x then return [] else return [readResultToEventKey x]

collectItemsToPage :: (GlobalFeedWriterStack q m) => Set EventKey -> Set EventKey -> Set EventKey -> m (Set EventKey)
collectItemsToPage _ acc newItems | null newItems = return acc
collectItemsToPage currentPage acc newItems = do
  let itemsNotInCurrentPage = Set.difference newItems currentPage
  log Debug ("acc:" <> show acc <> "currentPage: " <> show currentPage <> " newItems: " <> show newItems <> " itemsNotInCurrentPage: " <> show itemsNotInCurrentPage)
  previousUnpaged <- Set.fromList <$> getUnpagedPreviousEntries (toList itemsNotInCurrentPage)
  let previousUnpageNotInCurrentPage = Set.difference previousUnpaged acc
  collectItemsToPage currentPage (Set.union itemsNotInCurrentPage acc) previousUnpageNotInCurrentPage

dynamoKeyToEventKey :: DynamoKey -> EventKey
dynamoKeyToEventKey DynamoKey{..} =
  let
    streamId = StreamId $ T.drop (T.length Constants.streamDynamoKeyPrefix) dynamoKeyKey
  in EventKey (streamId, dynamoKeyEventNumber)

readResultToFeedEntry :: (GlobalFeedWriterStack q m) => DynamoReadResult -> m FeedEntry
readResultToFeedEntry readResult@DynamoReadResult{dynamoReadResultKey=DynamoKey{..}} = do
  itemEventCount <- entryEventCount readResult
  let (EventKey(streamId, eventNumber)) = readResultToEventKey readResult
  return $ FeedEntry streamId eventNumber itemEventCount

addItemsToGlobalFeed :: (GlobalFeedWriterStack q m) => [DynamoKey] -> m ()
addItemsToGlobalFeed [] = return ()
addItemsToGlobalFeed dynamoItemKeys = do
  log Debug ("addItemsToGlobalFeed: " <> show dynamoItemKeys)
  currentPage <- getCurrentPage
  let currentPageFeedEntries = feedPageEntries currentPage
  let currentPageEventKeys = Set.fromList . toList $ feedEntryToEventKey <$> currentPageFeedEntries
  let itemKeysSet = Set.fromList $ dynamoKeyToEventKey <$> dynamoItemKeys
  itemsToPage <- collectItemsToPage currentPageEventKeys Set.empty itemKeysSet
  items <- readItems . sort . toList $ itemsToPage
  let unpagedItems = filter (not . entryIsPaged) items
  newFeedEntries <- Seq.fromList <$> sequence (readResultToFeedEntry <$> unpagedItems)
  let version = feedPageVersion currentPage + 1
  let pageNumber = feedPageNumber currentPage
  log Debug ("writing to page: " <> show pageNumber <> " - currentFeedPageEntries: " <> show currentPageFeedEntries <> " newFeedEntries: " <> show newFeedEntries)
  let newEntrySequence = currentPageFeedEntries <> newFeedEntries
  pageResult <- writePage pageNumber newEntrySequence version
  onPageResult pageNumber pageResult newEntrySequence
  return ()
  where
    onPageResult :: GlobalFeedWriterStack q m => PageKey -> DynamoWriteResult -> Seq FeedEntry -> m ()
    onPageResult _ DynamoWriteWrongVersion _ = do
      log Debug "Got wrong version writing page"
      addItemsToGlobalFeed dynamoItemKeys
    onPageResult pageNumber DynamoWriteSuccess newEntrySequence =
      let
        toMarkAsPaged = feedEntryToDynamoKey <$> toList newEntrySequence
      in do
        trySetLatestPage pageNumber
        sequence_ $ (`setEventEntryPage` pageNumber) <$> toMarkAsPaged
    onPageResult pageNumber DynamoWriteFailure _ = throwError $ EventStoreActionErrorOnWritingPage pageNumber

data GlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage :: Maybe PageKey -- we don't always know the current page
}

emptyGlobalFeedWriterState :: GlobalFeedWriterState
emptyGlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage = Nothing
                                                   }

type GlobalFeedWriterStack q m = (MonadEsDsl q m, MonadError EventStoreActionError m, MonadState GlobalFeedWriterState m)

main :: (GlobalFeedWriterStack q m) => m ()
main = do
  scanResult <- scanNeedsPaging
  addItemsToGlobalFeed scanResult
  when (null scanResult) (wait 1000)
  setPulseStatus $ case scanResult of [] -> False
                                      _  -> True
