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
  getLastFullPage,
  emptyGlobalFeedWriterState,
  GlobalFeedWriterState(..),
  PageKeyPosition(..),
  DynamoCmdWithErrors,
  GlobalFeedPosition(..),
  EventStoreActionError(..)) where

import           Debug.Trace
import           BasicPrelude                          hiding (log)
import qualified Control.Foldl as Foldl
import           Control.Lens
import           Control.Monad.Except
import           Control.Monad.State
import qualified Data.Aeson                            as Aeson
import qualified Data.ByteString                       as BS
import qualified Data.ByteString.Lazy                  as BL
import           Data.Foldable
import qualified Data.HashMap.Lazy                     as HM
import qualified Data.Sequence                         as Seq
import           Data.List.NonEmpty                    (NonEmpty (..))
import qualified Data.List.NonEmpty                    as NonEmpty
import qualified Data.Set                              as Set
import qualified Data.Text                             as T
import qualified DynamoDbEventStore.Constants          as Constants
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.EventStoreQueries (streamEntryProducer)
import           DynamoDbEventStore.HeadEntry (getLastFullPage, getLastVerifiedPage, trySetLastFullPage)
import           DynamoDbEventStore.StreamEntry (streamEntryFirstEventNumber,StreamEntry(..))
import           DynamoDbEventStore.GlobalFeedItem (GlobalFeedItem(..), globalFeedItemsProducer,PageStatus(..),writeGlobalFeedItem, readPageMustExist, updatePageStatus, firstPageKey, readPage)
import           Pipes ((>->),Producer,runEffect)
import qualified Pipes.Prelude              as P
import           DynamoDbEventStore.Types
import           Network.AWS.DynamoDB                  hiding (updateItem)
import           Safe
import           Text.Printf                           (printf)

data FeedPage = FeedPage {
  feedPageNumber     :: PageKey,
  feedPageEntries    :: Seq.Seq FeedEntry,
  feedPageIsVerified :: Bool,
  feedPageVersion    :: Int
}

type DynamoCmdWithErrors q m = (MonadEsDsl m, MonadError EventStoreActionError m)

{-
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
-}

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

{-
readPageBody :: DynamoValues -> Seq.Seq FeedEntry
readPageBody values = -- todo don't ignore errors
  fromMaybe Seq.empty $ view (ix Constants.pageBodyKey . avB) values >>= Aeson.decodeStrict
-}

toDynamoKey :: StreamId -> Int64 -> DynamoKey
toDynamoKey (StreamId streamId) = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId)

eventKeyToDynamoKey :: EventKey -> DynamoKey
eventKeyToDynamoKey (EventKey(streamId, eventNumber)) = toDynamoKey streamId eventNumber

setEventEntryPage :: (MonadEsDsl m, MonadError EventStoreActionError m) => DynamoKey -> PageKey -> m ()
setEventEntryPage key (PageKey pageNumber) = do
    let updates =
          HM.fromList [
           (Constants.needsPagingKey, ValueUpdateDelete)
           , (Constants.eventPageNumberKey, ValueUpdateSet (set avS (Just (show pageNumber)) attributeValue))
                      ]
    updateItemWithRetry key updates

setFeedEntryPageNumber :: (MonadEsDsl m, MonadError EventStoreActionError m) => PageKey -> FeedEntry -> m ()
setFeedEntryPageNumber pageNumber feedEntry = do
  let streamId = feedEntryStream feedEntry
  let dynamoKey = toDynamoKey streamId  (feedEntryNumber feedEntry)
  void $ setEventEntryPage dynamoKey pageNumber

stringAttributeValue :: Text -> AttributeValue
stringAttributeValue t = set avS (Just t) attributeValue

verifyPage :: (MonadError EventStoreActionError m, MonadEsDsl m) => GlobalFeedItem -> m ()
verifyPage GlobalFeedItem{..} = do
  log Debug ("verifyPage " <> show globalFeedItemPageKey <> " got value " <> show globalFeedItemFeedEntries)
  void $ traverse (setFeedEntryPageNumber globalFeedItemPageKey) globalFeedItemFeedEntries
  updatePageStatus globalFeedItemPageKey PageStatusVerified

completeButUnverifiedGlobalFeedItemsProducer :: (MonadError EventStoreActionError m, MonadEsDsl m) => Producer GlobalFeedItem m ()
completeButUnverifiedGlobalFeedItemsProducer =
  globalFeedItemsProducer QueryDirectionForward Nothing
    >-> P.mapM waitUntilComplete
    >-> P.filter (\GlobalFeedItem{..} -> globalFeedItemPageStatus /= PageStatusVerified )
  where
    waitUntilComplete x@GlobalFeedItem { globalFeedItemPageStatus = PageStatusComplete } = return x
    waitUntilComplete x@GlobalFeedItem { globalFeedItemPageStatus = PageStatusVerified } = return x
    waitUntilComplete x@GlobalFeedItem { globalFeedItemPageStatus = PageStatusIncomplete } = do
      wait 1000
      result <- readPageMustExist (globalFeedItemPageKey x)
      waitUntilComplete result

verifyPagesThread :: MonadEsDsl m => m ()
verifyPagesThread =
  void $ runExceptT $ go firstPageKey
  where
    go pageKey = do
      result <- readPage pageKey
      --traceMe ("verifyPagesThread" <> show (globalFeedItemPageKey <$> result))
      maybe (pageDoesNotExist pageKey) pageExists result
    awaitPage pageKey = do
      setPulseStatus False
      wait 1000
      go pageKey
    pageDoesNotExist = awaitPage
    pageExists GlobalFeedItem { globalFeedItemPageStatus = PageStatusIncomplete, globalFeedItemPageKey = pageKey } =
      awaitPage pageKey
    pageExists x@GlobalFeedItem { globalFeedItemPageStatus = PageStatusVerified, globalFeedItemPageKey = pageKey } = go (succ pageKey)
    pageExists x@GlobalFeedItem { globalFeedItemPageStatus = PageStatusComplete, globalFeedItemPageKey = pageKey } =
      setPulseStatus True >> verifyPage x >> go (succ pageKey)
      
readFromDynamoMustExist :: GlobalFeedWriterStack q m => DynamoKey -> m DynamoReadResult
readFromDynamoMustExist key = do
  r <- readFromDynamo key
  case r of Just x -> return x
            Nothing -> throwError $ EventStoreActionErrorEventDoesNotExist key

emptyFeedPage :: PageKey -> FeedPage
emptyFeedPage pageNumber = FeedPage { feedPageNumber = pageNumber, feedPageEntries = Seq.empty, feedPageIsVerified = False, feedPageVersion = -1 }

pageSizeCutoff :: Int
pageSizeCutoff = 10

{-
getCurrentPage :: GlobalFeedWriterStack q m => m FeedPage
getCurrentPage = do
  mostRecentKnownPage <- gets globalFeedWriterStateCurrentPage
  mostRecentPage <- getMostRecentPage =<< maybe getLastFullPage return mostRecentKnownPage
  modify (\s -> s { globalFeedWriterStateCurrentPage = feedPageNumber <$> mostRecentPage})
  let mostRecentPageNumber = maybe (PageKey (-1)) feedPageNumber mostRecentPage
  let startNewPage = maybe True (\page -> (length . feedPageEntries) page >= pageSizeCutoff) mostRecentPage
  if startNewPage then do
    verifyPage mostRecentPageNumber
    return $ emptyFeedPage (mostRecentPageNumber + 1)
  else
    return $ fromMaybe (emptyFeedPage (mostRecentPageNumber + 1)) mostRecentPage
-}

readResultToDynamoKey :: DynamoReadResult -> DynamoKey
readResultToDynamoKey DynamoReadResult {..} = dynamoReadResultKey

readResultToEventKey :: DynamoReadResult -> EventKey
readResultToEventKey = dynamoKeyToEventKey . readResultToDynamoKey

readItems :: GlobalFeedWriterStack q m => [EventKey] -> m [DynamoReadResult]
readItems keys =
  sequence $ readFromDynamoMustExist . eventKeyToDynamoKey <$> keys

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

markEventAsPagedThread :: MonadEsDsl m => QueueType m (PageKey, FeedEntry) -> m ()
markEventAsPagedThread q = do
  (pageKey, feedEntry) <- readQueue q
  -- todo deal with this
  _ <- runExceptT $ setFeedEntryPageNumber pageKey feedEntry
  return ()

data ToBePaged =
  ToBePaged {
  toBePagedEntries           :: [FeedEntry],
  toBePagedVerifiedUpToPage  :: Maybe PageKey }

streamEntryToFeedEntry :: StreamEntry -> FeedEntry
streamEntryToFeedEntry StreamEntry{..} =
  FeedEntry {
    feedEntryStream = streamEntryStreamId,
    feedEntryNumber = streamEntryFirstEventNumber,
    feedEntryCount = length streamEntryEventEntries }

collectAncestors
  :: (MonadEsDsl m, MonadError EventStoreActionError m) =>
  DynamoKey ->
  m ToBePaged
collectAncestors dynamoKey =
  let
    EventKey(streamId, eventNumber) = dynamoKeyToEventKey dynamoKey
    streamFromEventBack = streamEntryProducer QueryDirectionBackward streamId (Just (eventNumber + 1)) 10
  in do
    lastVerifiedPage <- getLastVerifiedPage
    events <- P.toListM $  
                streamFromEventBack
                 >-> P.filter ((<= eventNumber) . streamEntryFirstEventNumber)
                 >-> P.map streamEntryToFeedEntry
    return $ ToBePaged events lastVerifiedPage

traceMe :: (Monad m, Show a) => a -> m ()
traceMe = traceM . T.unpack . show

-- todo: deal with errors
collectAncestorsThread ::
  (MonadEsDsl m) =>
  QueueType m DynamoKey ->
  QueueType m ToBePaged ->
  m ()
collectAncestorsThread inQ outQ = void $ runExceptT $ forever $ do
  i <- readQueue inQ
  result <- collectAncestors i
  writeQueue outQ result

data PageUpdate =
  PageUpdate {
    pageUpdatePageKey :: PageKey,
    pageUpdateNewEntries :: Seq FeedEntry,
    pageUpdatePageVersion :: DynamoVersion }
  deriving Show

writeItemsToPage
  :: (MonadEsDsl m, MonadError EventStoreActionError m) =>
  ToBePaged ->
  m (Maybe PageUpdate)
writeItemsToPage ToBePaged{..} =
  let
    toBePagedSet = Set.fromList . toList $ toBePagedEntries
    removePagedItem s GlobalFeedItem{..} =
      let pageItemSet = (Set.fromList . toList) globalFeedItemFeedEntries
      in Set.difference s pageItemSet
    filteredItemsToPage = Foldl.Fold removePagedItem toBePagedSet id
    combinedFold = (,) <$> filteredItemsToPage <*> Foldl.last
    foldOverProducer = Foldl.purely P.fold
    result = foldOverProducer combinedFold $ globalFeedItemsProducer QueryDirectionForward toBePagedVerifiedUpToPage
  in do
    (finalFeedEntries, lastPage) <- result
    let page@GlobalFeedItem{..} = getPageFromLast lastPage
    let sortedNewFeedEntries = (Seq.fromList . sort . toList) finalFeedEntries
    let page' = page { globalFeedItemFeedEntries = globalFeedItemFeedEntries <> sortedNewFeedEntries }
    _ <- writeGlobalFeedItem page' -- todo don't ignore errors
    return . Just $ PageUpdate {
      pageUpdatePageKey = globalFeedItemPageKey,
      pageUpdateNewEntries = sortedNewFeedEntries,
      pageUpdatePageVersion = globalFeedItemVersion}
  where
    getPageFromLast Nothing = GlobalFeedItem (PageKey 0) PageStatusIncomplete 0 Seq.empty
    getPageFromLast (Just globalFeedItem@GlobalFeedItem{ globalFeedItemPageStatus = PageStatusIncomplete, globalFeedItemVersion = version }) = globalFeedItem { globalFeedItemVersion = version + 1 }
    getPageFromLast (Just GlobalFeedItem{ globalFeedItemPageKey = pageKey }) =
      GlobalFeedItem {
        globalFeedItemPageKey = pageKey + 1,
        globalFeedItemPageStatus = PageStatusIncomplete,
        globalFeedItemVersion = 0,
        globalFeedItemFeedEntries = Seq.empty }

-- todo don't ignore errors
writeItemsToPageThread
  :: (MonadEsDsl m) =>
  QueueType m ToBePaged ->
  m ()
writeItemsToPageThread inQ = void . runExceptT . forever $ do
  item <- readQueue inQ
  result <- writeItemsToPage item
  --traceMe result
  --maybe (return()) (writeQueue outQ) result
  return ()

{-
addItemsToGlobalFeed :: (GlobalFeedWriterStack q m) => [DynamoKey] -> QueueType m (PageKey, FeedEntry) -> m ()
addItemsToGlobalFeed [] _ = return ()
addItemsToGlobalFeed dynamoItemKeys markFeedEntryPageQueue = do
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
    onPageResult _ DynamoWriteWrongVersion _ = do
      log Debug "Got wrong version writing page"
      addItemsToGlobalFeed dynamoItemKeys markFeedEntryPageQueue
    onPageResult pageNumber DynamoWriteSuccess newEntrySequence =
      let
        addFeedEntryToQueue feedEntry = writeQueue markFeedEntryPageQueue (pageNumber, feedEntry)
      in do
        trySetLastFullPage pageNumber
        sequence_ $ addFeedEntryToQueue <$> newEntrySequence
    onPageResult pageNumber DynamoWriteFailure _ = throwError $ EventStoreActionErrorOnWritingPage pageNumber
-}

data GlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage :: Maybe PageKey -- we don't always know the current page
}

emptyGlobalFeedWriterState :: GlobalFeedWriterState
emptyGlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage = Nothing
                                                   }

type GlobalFeedWriterStack q m = (MonadEsDsl m, MonadError EventStoreActionError m, MonadState GlobalFeedWriterState m)

forkChild' :: (MonadEsDslWithFork m) => Text -> m () -> StateT GlobalFeedWriterState (ExceptT EventStoreActionError m) ()

forkChild' threadName c = lift $ lift $ forkChild threadName c

data PageKeyPosition =
  PageKeyPositionLastComplete
  | PageKeyPositionLastVerified
  deriving (Eq, Ord, Show)

main :: MonadEsDslWithFork m => CacheType m PageKeyPosition PageKey -> StateT GlobalFeedWriterState (ExceptT EventStoreActionError m) ()
main _pagePositionCache = do
  itemsToPageQueue <- newQueue
  itemsReadyForGlobalFeed <- newQueue
  --markFeedEntryPageQueue <- newQueue
  --let startMarkFeedEntryThread = forkChild' (markEventAsPagedThread markFeedEntryPageQueue)
  --replicateM_ 25 startMarkFeedEntryThread
  let startCollectAncestorsThread = forkChild' "collectAncestorsThread" $ collectAncestorsThread itemsToPageQueue itemsReadyForGlobalFeed
  replicateM_ 25 startCollectAncestorsThread
  forkChild' "writeItemsToPageThread" $ writeItemsToPageThread itemsReadyForGlobalFeed
  forkChild' "verifyPagesThread" verifyPagesThread
  forever $ do
    scanResult <- scanNeedsPaging
    -- traceMe ("scanReuslt" <> show scanResult)
    _ <- traverse (writeQueue itemsToPageQueue) scanResult
    -- addItemsToGlobalFeed scanResult markFeedEntryPageQueue
    when (null scanResult) (wait 1000)
    let isActive = not (null scanResult)
    setPulseStatus isActive
