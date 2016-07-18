{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module DynamoDbEventStore.GlobalFeedWriter (
  main,
  FeedEntry(..),
  dynamoWriteWithRetry,
  entryEventCount,
  writePage,
  getPageDynamoKey,
  emptyGlobalFeedWriterState,
  GlobalFeedPosition(..),
  EventStoreActionError(..)) where

import           BasicPrelude
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
import           Network.AWS.DynamoDB
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
  EventStoreActionErrorInvalidGlobalFeedPage PageKey
  deriving (Show, Eq)

data GlobalFeedPosition = GlobalFeedPosition {
    globalFeedPositionPage   :: PageKey
  , globalFeedPositionOffset :: Int
} deriving (Show, Eq, Ord)

instance QC.Arbitrary GlobalFeedPosition where
  arbitrary = GlobalFeedPosition
                <$> QC.arbitrary
                <*> ((\(QC.Positive p) -> p) <$> QC.arbitrary)

data FeedEntry = FeedEntry {
  feedEntryStream :: StreamId,
  feedEntryNumber :: Int64,
  feedEntryCount  :: Int
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
  feedPageNumber     :: PageKey,
  feedPageEntries    :: Seq.Seq FeedEntry,
  feedPageIsVerified :: Bool,
  feedPageVersion    :: Int
}

dynamoWriteWithRetry :: DynamoKey -> DynamoValues -> Int -> ExceptT e DynamoCmdM DynamoWriteResult
dynamoWriteWithRetry key value version = loop 0 DynamoWriteFailure
  where
    loop :: Int -> DynamoWriteResult -> ExceptT e DynamoCmdM DynamoWriteResult
    loop 100 previousResult = return previousResult
    loop count DynamoWriteFailure = lift (writeToDynamo' key value version) >>= loop (count  + 1)
    loop _ previousResult = return previousResult

getPageDynamoKey :: PageKey -> DynamoKey
getPageDynamoKey (PageKey pageNumber) =
  let paddedPageNumber = T.pack (printf "%08d" pageNumber)
  in DynamoKey (Constants.pageDynamoKeyPrefix <> paddedPageNumber) 0

getMostRecentPage :: PageKey -> GlobalFeedWriterStack (Maybe FeedPage)
getMostRecentPage startPageNumber =
  readFeedPage startPageNumber >>= findPage
  where
    readFeedPage :: PageKey -> GlobalFeedWriterStack (Maybe FeedPage)
    readFeedPage pageNumber = do
      dynamoEntry <- lift (readFromDynamo' $ getPageDynamoKey pageNumber)
      return $ toFeedPage startPageNumber <$> dynamoEntry
    toFeedPage :: PageKey -> DynamoReadResult -> FeedPage
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

nextVersion :: DynamoReadResult -> Int
nextVersion readResult = dynamoReadResultVersion readResult + 1

toDynamoKey :: StreamId -> Int64 -> DynamoKey
toDynamoKey (StreamId streamId) = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId)

eventKeyToDynamoKey :: EventKey -> DynamoKey
eventKeyToDynamoKey (EventKey(streamId, eventNumber)) = toDynamoKey streamId eventNumber

setPageEntryPageNumber :: PageKey -> FeedEntry -> GlobalFeedWriterStack ()
setPageEntryPageNumber pageNumber feedEntry = do
  let streamId = feedEntryStream feedEntry
  let dynamoKey = toDynamoKey streamId  (feedEntryNumber feedEntry)
  eventEntry <- readFromDynamoMustExist dynamoKey
  let newValue = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (stringAttributeValue (show pageNumber)) . dynamoReadResultValue) eventEntry
  lift $ log' Debug "About to update page for "
  lift . void $ dynamoWriteWithRetry dynamoKey newValue (nextVersion eventEntry)

stringAttributeValue :: Text -> AttributeValue
stringAttributeValue t = set avS (Just t) attributeValue

verifyPage :: PageKey -> GlobalFeedWriterStack ()
verifyPage (PageKey (-1))       = return ()
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
    lift . void $ dynamoWriteWithRetry pageDynamoKey newValues (pageVersion + 1)

readFromDynamoMustExist :: DynamoKey -> GlobalFeedWriterStack DynamoReadResult
readFromDynamoMustExist key = do
  r <- readFromDynamo' key
  case r of Just x -> return x
            Nothing -> throwError $ EventStoreActionErrorEventDoesNotExist key

emptyFeedPage :: PageKey -> FeedPage
emptyFeedPage pageNumber = FeedPage { feedPageNumber = pageNumber, feedPageEntries = Seq.empty, feedPageIsVerified = False, feedPageVersion = -1 }

pageSizeCutoff :: Int
pageSizeCutoff = 10

getCurrentPage :: GlobalFeedWriterStack FeedPage
getCurrentPage = do
  mostRecentKnownPage <- gets globalFeedWriterStateCurrentPage
  mostRecentPage <- getMostRecentPage $ fromMaybe 0 mostRecentKnownPage
  modify (\s -> s { globalFeedWriterStateCurrentPage = feedPageNumber <$> mostRecentPage})
  let mostRecentPageNumber = maybe (PageKey (-1)) feedPageNumber mostRecentPage
  let startNewPage = maybe True (\page -> (length . feedPageEntries) page >= pageSizeCutoff) mostRecentPage
  if startNewPage then do
    verifyPage mostRecentPageNumber
    return $ emptyFeedPage (mostRecentPageNumber + 1)
  else
    return $ fromMaybe (emptyFeedPage (mostRecentPageNumber + 1)) mostRecentPage

setEventEntryPage :: DynamoKey -> PageKey -> GlobalFeedWriterStack DynamoWriteResult
setEventEntryPage key (PageKey pageNumber) = do
    eventEntry <- readFromDynamoMustExist key
    lift $ log' Debug ("have set page to " <> show pageNumber <> " for " <> show key)
    let values = dynamoReadResultValue eventEntry
    let version = dynamoReadResultVersion eventEntry
    let values' = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (set avS (Just (show pageNumber)) attributeValue)) values
    lift $ dynamoWriteWithRetry key values' (version + 1)

itemToJsonByteString :: Aeson.ToJSON a => a -> BS.ByteString
itemToJsonByteString = BL.toStrict . Aeson.encode . Aeson.toJSON

readResultToDynamoKey :: DynamoReadResult -> DynamoKey
readResultToDynamoKey DynamoReadResult {..} = dynamoReadResultKey

readResultToEventKey :: DynamoReadResult -> EventKey
readResultToEventKey = dynamoKeyToEventKey . readResultToDynamoKey

writePage :: PageKey -> Seq FeedEntry -> DynamoVersion -> GlobalFeedWriterStack DynamoWriteResult
writePage pageNumber entries version = do
  let feedEntry = itemToJsonByteString entries
  let dynamoKey = getPageDynamoKey pageNumber
  let body = HM.singleton Constants.pageBodyKey (set avB (Just feedEntry) attributeValue)
  lift $ dynamoWriteWithRetry dynamoKey body version

readItems :: [EventKey] -> GlobalFeedWriterStack [DynamoReadResult]
readItems keys =
  sequence $ readFromDynamoMustExist . eventKeyToDynamoKey <$> keys

feedEntryToDynamoKey :: FeedEntry -> DynamoKey
feedEntryToDynamoKey = eventKeyToDynamoKey . feedEntryToEventKey

feedEntryToEventKey :: FeedEntry -> EventKey
feedEntryToEventKey FeedEntry{..} = EventKey (feedEntryStream, feedEntryNumber )

getUnpagedPreviousEntries :: [EventKey] -> GlobalFeedWriterStack [EventKey]
getUnpagedPreviousEntries keys =
  join <$> sequence (getPreviousEntryIfUnpaged <$> keys)
  where
    getPreviousEntryIfUnpaged :: EventKey -> GlobalFeedWriterStack [EventKey]
    getPreviousEntryIfUnpaged (EventKey (_, 0)) = return []
    getPreviousEntryIfUnpaged eventKey = do
      let key = eventKeyToDynamoKey eventKey
      let DynamoKey{..} = key
      result <- lift $ queryTable' QueryDirectionBackward dynamoKeyKey 1 (Just dynamoKeyEventNumber)
      case result of [] -> throwError $ EventstoreActionErrorCouldNotFindPreviousEntry key
                     (x:_xs) -> if entryIsPaged x then return [] else return [readResultToEventKey x]

collectItemsToPage :: Set EventKey -> Set EventKey -> Set EventKey -> GlobalFeedWriterStack (Set EventKey)
collectItemsToPage _ acc newItems | null newItems = return acc
collectItemsToPage currentPage acc newItems = do
  let itemsNotInCurrentPage = Set.difference newItems currentPage
  lift $ log' Debug ("acc:" <> show acc <> "currentPage: " <> show currentPage <> " newItems: " <> show newItems <> " itemsNotInCurrentPage: " <> show itemsNotInCurrentPage)
  previousUnpaged <- Set.fromList <$> getUnpagedPreviousEntries (toList itemsNotInCurrentPage)
  let previousUnpageNotInCurrentPage = Set.difference previousUnpaged acc
  collectItemsToPage currentPage (Set.union itemsNotInCurrentPage acc) previousUnpageNotInCurrentPage

dynamoKeyToEventKey :: DynamoKey -> EventKey
dynamoKeyToEventKey DynamoKey{..} =
  let
    streamId = StreamId $ T.drop (T.length Constants.streamDynamoKeyPrefix) dynamoKeyKey
  in EventKey (streamId, dynamoKeyEventNumber)

readResultToFeedEntry :: DynamoReadResult -> GlobalFeedWriterStack FeedEntry
readResultToFeedEntry readResult@DynamoReadResult{dynamoReadResultKey=DynamoKey{..}} = do
  itemEventCount <- entryEventCount readResult
  let (EventKey(streamId, eventNumber)) = readResultToEventKey readResult
  return $ FeedEntry streamId eventNumber itemEventCount

addItemsToGlobalFeed :: [DynamoKey] -> GlobalFeedWriterStack ()
addItemsToGlobalFeed [] = return ()
addItemsToGlobalFeed dynamoItemKeys = do
  log' Debug ("addItemsToGlobalFeed: " <> show dynamoItemKeys)
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
  log' Debug ("writing to page: " <> show pageNumber <> " - currentFeedPageEntries: " <> show currentPageFeedEntries <> " newFeedEntries: " <> show newFeedEntries)
  let newEntrySequence = currentPageFeedEntries <> newFeedEntries
  pageResult <- writePage pageNumber newEntrySequence version
  onPageResult pageNumber pageResult newEntrySequence
  return ()
  where
    onPageResult :: PageKey -> DynamoWriteResult -> Seq FeedEntry -> GlobalFeedWriterStack ()
    onPageResult _ DynamoWriteWrongVersion _ = do
      log' Debug "Got wrong version writing page"
      addItemsToGlobalFeed dynamoItemKeys
    onPageResult pageNumber DynamoWriteSuccess newEntrySequence =
      let
        toMarkAsPaged = feedEntryToDynamoKey <$> toList newEntrySequence
      in sequence_ $ (`setEventEntryPage` pageNumber) <$> toMarkAsPaged
    onPageResult pageNumber DynamoWriteFailure _ = throwError $ EventStoreActionErrorOnWritingPage pageNumber

data GlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage :: Maybe PageKey -- we don't always know the current page
}

emptyGlobalFeedWriterState :: GlobalFeedWriterState
emptyGlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage = Nothing
                                                   }

type GlobalFeedWriterStack = StateT GlobalFeedWriterState (ExceptT EventStoreActionError DynamoCmdM)

runLoop :: GlobalFeedWriterStack ()
runLoop = do
  scanResult <- lift scanNeedsPaging'
  addItemsToGlobalFeed scanResult
  when (null scanResult) (wait' 1000)
  setPulseStatus' $ case scanResult of [] -> False
                                       _  -> True
main :: DynamoCmdM (Either EventStoreActionError ())
main = do
  result <- runExceptT $ evalStateT runLoop emptyGlobalFeedWriterState
  case result of (Left errMsg) -> do
                                   log' Debug ("Terminating with error: " <> show errMsg)
                                   return $ Left errMsg
                 (Right ())    -> main
