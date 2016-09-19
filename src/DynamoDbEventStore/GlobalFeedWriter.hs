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

import           BasicPrelude                          hiding (log)
import           Control.Exception (throw)
import qualified Control.Foldl as Foldl
import           Control.Lens
import           Data.List.NonEmpty (NonEmpty(..))
import           Control.Monad.Except
import           Control.Monad.State
import           Data.Foldable
import qualified Data.HashMap.Lazy                     as HM
import qualified Data.Sequence                         as Seq
import qualified Data.Set                              as Set
import qualified Data.Text                             as T
import qualified DynamoDbEventStore.Constants          as Constants
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.EventStoreQueries (streamEntryProducer)
import           DynamoDbEventStore.HeadEntry (getLastFullPage, getLastVerifiedPage, trySetLastVerifiedPage)
import           DynamoDbEventStore.StreamEntry (streamEntryFirstEventNumber,StreamEntry(..), getStreamIdFromDynamoKey)
import           DynamoDbEventStore.GlobalFeedItem (GlobalFeedItem(..), globalFeedItemsProducer,PageStatus(..),writeGlobalFeedItem, updatePageStatus, firstPageKey, readPage)
import           Pipes ((>->),Producer,yield)
import qualified Pipes.Prelude              as P
import           DynamoDbEventStore.Types
import           Network.AWS.DynamoDB                  hiding (updateItem)
import           Safe

type DynamoCmdWithErrors q m = (MonadEsDsl m, MonadError EventStoreActionError m)

entryEventCount :: (MonadError EventStoreActionError m) => DynamoReadResult -> m Int
entryEventCount dynamoItem =
  let
    value = dynamoItem &
              dynamoReadResultValue &
              view (ix Constants.eventCountKey . avN)
    parsedValue = value >>= (Safe.readMay . T.unpack)
  in case parsedValue of Nothing  -> throwError $ EventStoreActionErrorCouldNotReadEventCount value
                         (Just x) -> return x

toDynamoKey :: StreamId -> Int64 -> DynamoKey
toDynamoKey (StreamId streamId) = DynamoKey (Constants.streamDynamoKeyPrefix <> streamId)

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

verifyPage :: (MonadError EventStoreActionError m, MonadEsDsl m) => GlobalFeedItem -> m ()
verifyPage GlobalFeedItem{..} = do
  void $ traverse (setFeedEntryPageNumber globalFeedItemPageKey) globalFeedItemFeedEntries
  updatePageStatus globalFeedItemPageKey PageStatusVerified
  trySetLastVerifiedPage globalFeedItemPageKey

verifyPagesThread :: MonadEsDsl m => m ()
verifyPagesThread =
  throwOnLeft $ go firstPageKey
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
    pageExists GlobalFeedItem { globalFeedItemPageStatus = PageStatusVerified, globalFeedItemPageKey = pageKey } = go (succ pageKey)
    pageExists x@GlobalFeedItem { globalFeedItemPageStatus = PageStatusComplete, globalFeedItemPageKey = pageKey } =
      setPulseStatus True >> verifyPage x >> go (succ pageKey)

data ToBePaged =
  ToBePaged {
  toBePagedEntries           :: [FeedEntry],
  toBePagedVerifiedUpToPage  :: Maybe PageKey }
  deriving (Show)

instance Monoid ToBePaged where
  mempty = ToBePaged {
    toBePagedEntries = mempty,
    toBePagedVerifiedUpToPage = Nothing }
  mappend
    ToBePaged { toBePagedEntries = toBePagedEntries1, toBePagedVerifiedUpToPage = toBePagedVerifiedUpToPage1 }
    ToBePaged { toBePagedEntries = toBePagedEntries2, toBePagedVerifiedUpToPage = toBePagedVerifiedUpToPage2 } = ToBePaged (toBePagedEntries1 <> toBePagedEntries2) (minPageKey toBePagedVerifiedUpToPage1 toBePagedVerifiedUpToPage2)
    where
      minPageKey Nothing other = other
      minPageKey other Nothing = other
      minPageKey (Just pk1) (Just pk2) = Just $ min pk1 pk2
    
streamEntryToFeedEntry :: StreamEntry -> FeedEntry
streamEntryToFeedEntry StreamEntry{..} =
  FeedEntry {
    feedEntryStream = streamEntryStreamId,
    feedEntryNumber = streamEntryFirstEventNumber,
    feedEntryCount = length streamEntryEventEntries }

collectAncestors
  :: (MonadEsDsl m, MonadError EventStoreActionError m) =>
  StreamId ->
  m ToBePaged
collectAncestors streamId =
  let
    streamFromEventBack = streamEntryProducer QueryDirectionBackward streamId Nothing 10
  in do
    lastVerifiedPage <- getLastVerifiedPage
    events <- P.toListM $  
                streamFromEventBack
                 >-> P.takeWhile streamEntryNeedsPaging
                 >-> P.map streamEntryToFeedEntry
    return $ ToBePaged events lastVerifiedPage

collectAncestorsThread ::
  (MonadEsDsl m) =>
  QueueType m StreamId ->
  QueueType m ToBePaged ->
  m ()
collectAncestorsThread inQ outQ =
  throwOnLeft $ forever $ do
    i <- readQueue inQ
    result <- collectAncestors i
    writeQueue outQ result

data PageUpdate =
  PageUpdate {
    pageUpdatePageKey :: PageKey,
    pageUpdateNewEntries :: Seq FeedEntry,
    pageUpdatePageVersion :: DynamoVersion }
  deriving Show

data FeedPage = FeedPage {
  feedPageKey :: PageKey,
  feedPageItems :: Set FeedEntry }

feedPageProducerForward :: (MonadEsDsl m, MonadError EventStoreActionError m)
  => CacheType m PageKey (Set FeedEntry) 
  -> Maybe PageKey
  -> Producer FeedPage m ()
feedPageProducerForward completePageCache Nothing = feedPageProducerForward completePageCache (Just firstPageKey)
feedPageProducerForward completePageCache (Just page) = do
  cacheResult <- lift $ cacheLookup completePageCache page
  maybe lookupDb yieldAndLoop cacheResult
  where
    lookupDb =
      globalFeedItemsProducer QueryDirectionForward False (Just page)
      >->
      P.map globalFeedItemToFeedPage 
    globalFeedItemToFeedPage GlobalFeedItem{..} =
      FeedPage globalFeedItemPageKey (Set.fromList . toList $ globalFeedItemFeedEntries)
    yieldAndLoop feedEntries = do
      yield $ FeedPage page feedEntries
      feedPageProducerForward completePageCache (Just $ page + 1)

writeItemsToPage
  :: (MonadEsDsl m, MonadError EventStoreActionError m) =>
  CacheType m PageKey (Set FeedEntry) ->
  ToBePaged ->
  m (Maybe PageUpdate)
writeItemsToPage completePageCache ToBePaged{..} =
  let
    toBePagedSet = Set.fromList . toList $ toBePagedEntries
    removePagedItem s FeedPage{..} = Set.difference s feedPageItems
    filteredItemsToPage = Foldl.Fold removePagedItem toBePagedSet id
    combinedFold = (,) <$> filteredItemsToPage <*> Foldl.last
    foldOverProducer = Foldl.purely P.fold
    result = foldOverProducer combinedFold $ feedPageProducerForward completePageCache toBePagedVerifiedUpToPage
  in do
    (finalFeedEntries, lastPage) <- result
    let pageKey = getNextPageKey lastPage
    let sortedNewFeedEntries = (Seq.fromList . sort . toList) finalFeedEntries
    let pageVersion = 0
    let page = GlobalFeedItem {
          globalFeedItemPageKey = pageKey,
          globalFeedItemPageStatus = PageStatusComplete,
          globalFeedItemVersion = pageVersion,
          globalFeedItemFeedEntries = sortedNewFeedEntries }
    _ <- writeGlobalFeedItem page -- todo don't ignore errors
    cacheInsert completePageCache pageKey (Set.fromList . toList $ sortedNewFeedEntries)
    log Debug ("paged: " <> (show . length) sortedNewFeedEntries)
    return . Just $ PageUpdate {
      pageUpdatePageKey = pageKey,
      pageUpdateNewEntries = sortedNewFeedEntries,
      pageUpdatePageVersion = pageVersion }
  where
    getNextPageKey Nothing = firstPageKey
    getNextPageKey (Just FeedPage {..}) = feedPageKey + 1

throwOnLeft :: MonadEsDsl m => ExceptT EventStoreActionError m () -> m ()
throwOnLeft action = do
  result <- runExceptT action
  case result of Left e   -> do
                   log Error (show e)
                   throw e
                 Right () -> return ()

collectAllAvailable :: (Typeable a,  MonadEsDsl m) => QueueType m a -> m (NonEmpty a)
collectAllAvailable q = do
  firstItem <- readQueue q
  moreItems <- tryReadMore []
  return $ firstItem :| moreItems
  where
    tryReadMore acc = do
      result <- tryReadQueue q
      maybe (return acc) (\x -> tryReadMore (x:acc)) result
  
writeItemsToPageThread
  :: (MonadEsDsl m) =>
  CacheType m PageKey (Set FeedEntry) ->
  QueueType m ToBePaged ->
  m ()
writeItemsToPageThread completePageCache inQ = throwOnLeft . forever $ do
  items <- collectAllAvailable inQ
  _ <- writeItemsToPage completePageCache (fold items)
  return ()

data GlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage :: Maybe PageKey -- we don't always know the current page
}

emptyGlobalFeedWriterState :: GlobalFeedWriterState
emptyGlobalFeedWriterState = GlobalFeedWriterState {
  globalFeedWriterStateCurrentPage = Nothing
                                                   }

forkChild' :: (MonadEsDslWithFork m) => Text -> m () -> StateT GlobalFeedWriterState (ExceptT EventStoreActionError m) ()

forkChild' threadName c = lift $ lift $ forkChild threadName c

data PageKeyPosition =
  PageKeyPositionLastComplete
  | PageKeyPositionLastVerified
  deriving (Eq, Ord, Show)

scanNeedsPagingIndex :: MonadEsDsl m => QueueType m StreamId -> m ()
scanNeedsPagingIndex itemsToPageQueue =
  let
    go cache = do
      scanResult <- scanNeedsPaging
      (filteredScan :: [DynamoKey]) <- filterM (notInCache cache) scanResult
      let streams = toList . Set.fromList $ getStreamIdFromDynamoKey <$> filteredScan
      _ <- traverse (writeQueue itemsToPageQueue) streams
      unless (null filteredScan) (log Debug $ "Scanned new:" <> (show . length) filteredScan)
      when (null scanResult) (wait 1000)
      let isActive = not (null scanResult)
      setPulseStatus isActive
      _ <- traverse (\k -> cacheInsert cache k True) filteredScan
      go cache
    notInCache cache dynamoKey = do
      result <- cacheLookup cache dynamoKey
      return $ isNothing result
  in do
    cache <- newCache 100000
    go cache

main :: MonadEsDslWithFork m => CacheType m PageKeyPosition PageKey -> StateT GlobalFeedWriterState (ExceptT EventStoreActionError m) ()
main _pagePositionCache = do
  itemsToPageQueue <- newQueue
  itemsReadyForGlobalFeed <- newQueue
  completePageCache <- newCache 1000
  let startCollectAncestorsThread = forkChild' "collectAncestorsThread" $ collectAncestorsThread itemsToPageQueue itemsReadyForGlobalFeed
  replicateM_ 100 startCollectAncestorsThread
  forkChild' "writeItemsToPageThread" $ writeItemsToPageThread completePageCache itemsReadyForGlobalFeed
  forkChild' "verifyPagesThread" verifyPagesThread
  scanNeedsPagingIndex itemsToPageQueue
