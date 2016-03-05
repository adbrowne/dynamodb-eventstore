{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module GlobalFeedWriter (main, FeedEntry(FeedEntry)) where

import           Control.Monad
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as T
import qualified Data.Text.Lazy        as TL
import qualified Data.ByteString.Lazy  as BL
import qualified Data.Text.Lazy.Builder        as TL
import qualified Data.HashMap.Lazy as HM
import qualified DynamoDbEventStore.Constants as Constants
import           EventStoreCommands
import           Data.Maybe
import           Data.Monoid
import           Control.Lens
import           Network.AWS.DynamoDB
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode as Aeson
import           Control.Applicative
import           Data.Text.Format (format)
import           Text.Printf (printf)

toText :: Show s => s -> T.Text
toText = T.pack . show

data FeedEntry = FeedEntry {
  feedEntryStream :: T.Text,
  feedEntryNumber :: Int
} deriving (Eq, Show)

instance Aeson.FromJSON FeedEntry where
    parseJSON (Aeson.Object v) = FeedEntry <$>
                           v Aeson..: "s" <*>
                           v Aeson..: "n"
    parseJSON _                = empty

instance Aeson.ToJSON FeedEntry where
    toJSON (FeedEntry stream number) =
        Aeson.object ["s" Aeson..= stream, "n" Aeson..= number]

dynamoWriteWithRetry :: DynamoKey -> DynamoValues -> Maybe Int -> DynamoCmdM DynamoWriteResult
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

getMostRecentPage :: Int -> DynamoCmdM Int
getMostRecentPage startPage = do
  eventEntry <- readFromDynamo' (getPageDynamoKey startPage)
  case eventEntry of Just _  -> getMostRecentPage (startPage + 1)
                     Nothing -> return (startPage - 1)

entryIsPaged :: DynamoKey -> DynamoCmdM Bool
entryIsPaged item = do
  dynamoItem <- readFromDynamoMustExist item
  return ((not . containsNeedsPagingKey) dynamoItem)
  where 
    containsNeedsPagingKey :: DynamoReadResult -> Bool
    containsNeedsPagingKey = HM.member Constants.needsPagingKey . dynamoReadResultValue
  
previousEntryIsPaged :: DynamoKey -> DynamoCmdM Bool
previousEntryIsPaged item = 
  let itemEventNumber = dynamoKeyEventNumber item
  in
    if itemEventNumber == 0 then
      return True
    else
      entryIsPaged (item { dynamoKeyEventNumber = itemEventNumber - 1})

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

readPageBody :: DynamoValues -> [FeedEntry]
readPageBody values = 
  fromMaybe [] $ do -- todo don't ignore errors
    body <- HM.lookup Constants.pageBodyKey values
    (bodyString :: T.Text) <- view avS body
    Aeson.decode (BL.fromStrict (T.encodeUtf8 bodyString))

{-
  def readPageBody(values : EventStoreDsl.DynamoValue) : List[FeedEntry] = 
    values.get(Constants.PageBodyKey).flatMap { case DynamoString(body) => body.decodeOption[List[FeedEntry]]}.getOrElse(List.empty) // todo don't ignore errors

-}


nextVersion :: DynamoReadResult -> Maybe Int
nextVersion readResult = Just $ dynamoReadResultVersion readResult + 1

setPageEntryPageNumber :: Int -> FeedEntry -> DynamoCmdM ()
setPageEntryPageNumber pageNumber feedEntry = do
  let dynamoKey = DynamoKey (feedEntryStream feedEntry) (feedEntryNumber feedEntry)
  eventEntry <- readFromDynamoMustExist dynamoKey
  let newValue = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (stringAttributeValue (toText pageNumber)) . dynamoReadResultValue) eventEntry
  void $ dynamoWriteWithRetry dynamoKey newValue (nextVersion eventEntry)

checkItemPaged :: DynamoKey -> DynamoCmdM Bool 
checkItemPaged item = do
  eventEntry <- readFromDynamoMustExist item
  log' Debug ("checkItemPaged " <> toText eventEntry)
  return $ (HM.member Constants.eventPageNumberKey . dynamoReadResultValue ) eventEntry
  
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
    void $ dynamoWriteWithRetry pageDynamoKey newValues (Just (pageVersion + 1))

logIf :: Bool -> LogLevel -> T.Text -> DynamoCmdM ()
logIf True logLevel t = log' logLevel t
logIf False _ _ = return ()

asJsonText :: Aeson.ToJSON a => a -> T.Text
asJsonText = TL.toStrict . TL.toLazyText . Aeson.encodeToTextBuilder . Aeson.toJSON 

readFromDynamoMustExist :: DynamoKey -> DynamoCmdM DynamoReadResult
readFromDynamoMustExist key = do
  r <- readFromDynamo' key 
  case r of Just x -> return x
            Nothing -> fatalError' ("Could not find item: " <> toText key)

updateGlobalFeed :: DynamoKey -> DynamoCmdM ()
updateGlobalFeed item@DynamoKey { dynamoKeyKey = itemKey, dynamoKeyEventNumber = itemEventNumber } = do
  log' Debug ("updateGlobalFeed" <> toText item)
  mostRecentPage <- getMostRecentPage 0
  verifyPage mostRecentPage
  itemIsPaged <- checkItemPaged item
  logIf itemIsPaged Debug ("itemIsPaged" <> toText item)
  unless itemIsPaged $ do
    let feedEntry = asJsonText [FeedEntry  itemKey itemEventNumber]
    let nextPage = mostRecentPage + 1
    when (dynamoKeyEventNumber item > 0) (updateGlobalFeed item { dynamoKeyEventNumber = itemEventNumber - 1 })
    pageResult <- dynamoWriteWithRetry (getPageDynamoKey nextPage) (HM.singleton Constants.pageBodyKey (set avS (Just feedEntry) attributeValue)) Nothing
    onPageResult nextPage pageResult
    return ()
  return ()
  where 
    onPageResult :: Int -> DynamoWriteResult -> DynamoCmdM ()
    onPageResult _ DynamoWriteWrongVersion = do
      log' Debug "Got wrong version writing page"
      updateGlobalFeed item
    onPageResult nextPage DynamoWriteSuccess = do
      eventEntry <- readFromDynamoMustExist item 
      let values = dynamoReadResultValue eventEntry
      let version = dynamoReadResultVersion eventEntry
      let values' = (HM.delete Constants.needsPagingKey . HM.insert Constants.eventPageNumberKey (set avS (Just (toText nextPage)) attributeValue)) values
      itemUpdateResult <- dynamoWriteWithRetry item values' (Just (version + 1))
      when (itemUpdateResult == DynamoWriteSuccess) (verifyPage nextPage)
    onPageResult _ DynamoWriteFailure = undefined

writeItemToGlobalFeed :: DynamoKey -> DynamoCmdM ()
writeItemToGlobalFeed item = do
  previousEntryOk <- previousEntryIsPaged item
  entryPaged <- entryIsPaged item
  log' Debug ("entryPaged: " <> toText entryPaged <> " previousEntryOk " <> toText previousEntryOk)
  when (previousEntryOk && not entryPaged) (updateGlobalFeed item)

main :: DynamoCmdM ()
main = forever $ do
  scanResult <- scanNeedsPaging'
  forM_ scanResult writeItemToGlobalFeed
  log' Debug $ (toText . length) scanResult
  setPulseStatus' $ case scanResult of [] -> False
                                       _  -> True
