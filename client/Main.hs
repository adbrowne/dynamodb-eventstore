{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           BasicPrelude
import           Control.Concurrent
import           Control.Lens                    hiding (children, element)
import           Data.Aeson
import           Data.Aeson.Diff
import           Data.Aeson.Encode
import           Data.Aeson.Lens
import           Data.Attoparsec.ByteString.Lazy
import qualified Data.ByteString                 as B
import           Data.ByteString.Builder
import           Data.Char                       (isAlphaNum)
import qualified Data.HashMap.Strict             as HM
import           Data.Maybe
import qualified Data.Text                       as T
import qualified Data.Text.Encoding              as T
import           Data.Text.Encoding.Error        (lenientDecode)
import qualified Data.Text.Lazy                  as TL
import           Data.Text.Lazy.Encoding         (decodeUtf8With)
import qualified Data.UUID                       (UUID)
import           Network.Wreq
import qualified Options.Applicative             as Opt
import           System.Random
import           Text.Blaze.Renderer.Pretty
import           Text.Taggy.DOM
import           Text.Taggy.Lens
import           Text.Taggy.Renderer
import           Turtle.Prelude

getFeedPage :: Text -> IO (Response LByteString)
getFeedPage url = do
  let opts =
        defaults
        & header "Accept" .~ ["application/atom+xml"]
        & auth ?~ basicAuth "admin" "changeit"
  getWith opts (T.unpack url)

getEntry :: Bool -> Text -> IO (Response LByteString)
getEntry isEmbedTryHarder url = do
  let opts =
        defaults
        & header "Accept" .~ ["application/vnd.eventstore.atom+json"]
        & auth ?~ basicAuth "admin" "changeit"
  let getUrl = if isEmbedTryHarder then
                 T.unpack url <> "?embed=tryharder"
               else T.unpack url
  getWith opts getUrl

getRelLinks :: Text -> LText -> [Text]
getRelLinks relName =
  catMaybes . toListOf
    (html
    . allNamed (only "link")
    . attributed (ix "rel" . only relName)
    . attr "href")

getBodyAsText :: Response LByteString -> LText
getBodyAsText r = r ^. (responseBody . to (decodeUtf8With lenientDecode))

extractPrevious :: Text -> IO ([Text], [Text], Response LByteString)
extractPrevious url = do
  r <- getFeedPage url
  let bodyText = getBodyAsText r
  let feedLinks = getRelLinks "previous" bodyText
  let feedEntries = getRelLinks "edit" bodyText
  return (feedLinks, feedEntries, r)

followPrevious :: Text -> IO [([Text],Response LByteString)]
followPrevious startUrl = go [startUrl]
  where
    go [] = return []
    go urls = do
      results <- sequence $ extractPrevious <$> urls
      let nextUrls = join $ (\(a,_b,_c) -> a) <$> results
      let responses = (\(_a,b,c) -> (b,c)) <$> results
      nextResponses <- go nextUrls
      return $ responses ++ nextResponses

data Config = Config
  { configCommand :: Command }

data Command
  = DownloadGlobalStream DownloadGlobalStreamConfig
    | CopyGlobalStream CopyGlobalStreamConfig
    | CompareDownload CompareDownloadConfig

data DownloadGlobalStreamConfig = DownloadGlobalStreamConfig {
  downloadGlobalStreamOutputDirectory :: Text,
  downloadGlobalStreamStartUrl        :: Text }

data CopyGlobalStreamConfig = CopyGlobalStreamConfig {
  copyGlobalStreamConfigStartUrl    :: Text,
  copyGlobalStreamConfigDestination :: Text }

data CompareDownloadConfig = CompareDownloadConfig {
  compareDownloadConfigLeftDirectory  :: String,
  compareDownloadConfigRightDirectory :: String }

config :: Opt.Parser Config
config = Config
   <$> Opt.subparser
         (Opt.command "downloadGlobalStream" (Opt.info downloadGlobalStreamOptions (Opt.progDesc "Download global stream moving forward in time") )
         <>
          Opt.command "copyGlobalStream" (Opt.info copyGlobalStreamOptions (Opt.progDesc "Copy global stream"))
         <>
          Opt.command "compareDownload" (Opt.info compareDownloadOptions (Opt.progDesc "Compare downloaded streams"))
         )
   where
     downloadGlobalStreamOptions :: Opt.Parser Command
     downloadGlobalStreamOptions = DownloadGlobalStream <$>
       (DownloadGlobalStreamConfig
        <$> (T.pack <$> Opt.strOption (Opt.long "outputDirectory"
            <> Opt.metavar "OUTPUTDIRECTORY"
            <> Opt.help "ouput directory for responses"))
        <*> (T.pack <$> Opt.strOption (Opt.long "startUrl"
            <> Opt.metavar "STARTURL"
            <> Opt.help "starting url"))
       )
     copyGlobalStreamOptions :: Opt.Parser Command
     copyGlobalStreamOptions = CopyGlobalStream <$>
       (CopyGlobalStreamConfig
        <$> (T.pack <$> Opt.strOption (Opt.long "startUrl"
            <> Opt.metavar "STARTURL"
            <> Opt.help "starting url"))
        <*> (T.pack <$> Opt.strOption (Opt.long "destination"
            <> Opt.metavar "destination"
            <> Opt.help "destination event store - base url"))
       )
     compareDownloadOptions :: Opt.Parser Command
     compareDownloadOptions = CompareDownload <$>
       (CompareDownloadConfig
        <$> Opt.strOption (Opt.long "referenceDirectory"
            <> Opt.metavar "REFERENCE_DIRECTORY"
            <> Opt.help "reference directory")
        <*> Opt.strOption (Opt.long "testDirectory"
            <> Opt.metavar "TEST_DIRECTORY"
            <> Opt.help "test directory")
       )

saveResponse :: Text -> Text -> Response LByteString -> IO ()
saveResponse directory filename response = do
  let fileContent =
        response
        ^. (responseBody
            . to (decodeUtf8With lenientDecode)
            . to TL.toStrict
           )
  mktree (fromString $ T.unpack directory )
  writeTextFile (fromString $ T.unpack filename) fileContent

outputResponse :: Text -> Text -> (Int, ([Text],Response LByteString)) -> IO ()
outputResponse baseDir responseType (sequenceNumber, (_editLinks, response)) = do
  let directory = baseDir <> "/" <> responseType <> "/"
  let filename = directory <> show sequenceNumber
  saveResponse directory filename response

outputEntry :: Text -> Text -> (Int, Response LByteString) -> IO ()
outputEntry baseDir responseType (sequenceNumber, response) = do
  let directory = baseDir <> "/" <> responseType <> "/entries/"
  let filename = directory <> show sequenceNumber
  saveResponse directory filename response

data EntryData = EntryData {
  entryDataType     :: Text
  , entryDataStream :: Text
  , entryDataBody   :: Maybe LByteString}
  deriving Show

toEntryData :: Response LByteString -> EntryData
toEntryData r =
  let
    body = view responseBody r
  in fromJust $ do
    jsonValue <- maybeResult . parse json $ body
    streamId <- preview (key "streamId" . _String) jsonValue
    eventType <- preview (key "eventType" . _String) jsonValue
    let dataBody = preview (
          key "content"
          . key "data"
          . to (toLazyByteString . encodeToBuilder)
          ) jsonValue
    return EntryData {
      entryDataType = eventType
      , entryDataStream = streamId
      , entryDataBody = dataBody
    }

putEntry :: Text -> EntryData -> IO()
putEntry destinationBaseUrl EntryData{..} = do
  threadDelay 1000000
  let url = T.unpack $ destinationBaseUrl <> "/streams/" <> entryDataStream
  let body = fromMaybe (fromString "") entryDataBody
  (eventId :: Data.UUID.UUID) <- randomIO
  print $ "Entry: " <> entryDataStream <> " " <> entryDataType
  let opts = defaults
        & header "ES-EventType" .~ [T.encodeUtf8 entryDataType]
        & header "ES-EventId" .~ [T.encodeUtf8 $ show eventId]
        & header "Accept" .~ ["application/vnd.eventstore.atom+json"]
  _ <- postWith opts url body
  return ()

readXmlFile :: String -> String -> IO Node
readXmlFile directory subPath = do
  let filePath = directory <> subPath
  fileContents <- T.strip <$> readTextFile (fromString filePath)
  return $ head $ parseDOM False (TL.fromStrict fileContents)

readJsonFile :: String -> String -> IO Value
readJsonFile directory subPath = do
  let filePath = directory <> subPath
  fileContents <- B.readFile (fromString filePath)
  return $ fromJust $ fileContents ^? _Value

normalizeDom :: Node -> Maybe Node
normalizeDom n@(NodeContent _) = Just n
normalizeDom (NodeElement el@Element{eltName = "link", ..}) =
  let
    eltAttrs' = HM.adjust (const "normalized") "href" eltAttrs
    relValue = HM.lookup "rel" eltAttrs
  in
    if relValue == Just "metadata" || relValue == Just "last" then
      Nothing
    else
      Just $ NodeElement el { eltAttrs = eltAttrs', eltChildren = catMaybes $ normalizeDom <$> eltChildren}
normalizeDom (NodeElement el@Element{eltName = "updated", ..}) =
  Just $ NodeElement el { eltChildren = [] }
normalizeDom (NodeElement el@Element{..}) =
  Just $ NodeElement el { eltChildren = catMaybes $ normalizeDom <$> eltChildren }

renderNode :: Node -> Text
renderNode node =
  T.pack $ renderMarkup $ toMarkup False node

compareFeedFiles :: CompareDownloadConfig -> String -> IO Bool
compareFeedFiles CompareDownloadConfig{..} filePath = do
  domLeft <- normalizeDom <$> readXmlFile compareDownloadConfigLeftDirectory filePath
  domRight <- normalizeDom <$> readXmlFile compareDownloadConfigRightDirectory filePath
  let areEqual = domLeft == domRight
  print areEqual
  unless areEqual $ do
    putStrLn ("Left DOM:" :: Text)
    putStrLn $ domToText domLeft
    putStrLn ("Right DOM:" :: Text)
    putStrLn $ domToText domRight
  return areEqual
  where domToText n = fromMaybe "" $ renderNode <$> n

normalizeJson :: Value -> Value
normalizeJson =
  let
    removeMetadata = over (key "content" . _Object) (HM.delete "metadata")
    replaceDigitWithZero :: Char -> Char
    replaceDigitWithZero a | isAlphaNum a = '0'
    replaceDigitWithZero a = a
    zeroOutAlphaNum myPrism = over myPrism (T.map replaceDigitWithZero)
  in
    removeMetadata
    . zeroOutAlphaNum (key "content" . key "eventId" . _String)
    . zeroOutAlphaNum (key "updated" . _String)

compareEntryFile :: CompareDownloadConfig -> String -> IO Bool
compareEntryFile CompareDownloadConfig{..} filePath = do
  domLeft <- normalizeJson <$> readJsonFile compareDownloadConfigLeftDirectory filePath
  domRight <- normalizeJson <$> readJsonFile compareDownloadConfigRightDirectory filePath
  let areEqual = domLeft == domRight
  print areEqual
  unless areEqual $ do
    putStrLn ("Left DOM:" :: Text)
    print domLeft
    putStrLn ("Right DOM:" :: Text)
    print domRight
    putStrLn "Diff:"
    print $ diff domLeft domRight
  return areEqual

start :: Config -> IO ()
start Config { configCommand = DownloadGlobalStream DownloadGlobalStreamConfig{..}} = do
  responses <- followPrevious downloadGlobalStreamStartUrl
  let numberedResponses = zip [0..] responses
  entryBodies <- sequence $ getEntry False <$> join (fst <$> responses)
  let numberedEntries = zip [0..] entryBodies
  void $ sequence $ outputResponse downloadGlobalStreamOutputDirectory "previous" <$> numberedResponses
  void $ sequence $ outputEntry downloadGlobalStreamOutputDirectory "previous" <$> numberedEntries
start Config { configCommand = CopyGlobalStream CopyGlobalStreamConfig{..} } = do
  responses <- followPrevious copyGlobalStreamConfigStartUrl
  entryBodies <- sequence $ getEntry True <$> join (fst <$> responses)
  let entryData = reverse $ toEntryData <$> entryBodies
  sequence_ $ putEntry copyGlobalStreamConfigDestination <$> entryData
  return ()
start Config { configCommand = CompareDownload compareDownloadConfig } = do
  void $ compareFeedFiles compareDownloadConfig "previous/0"
  void $ compareFeedFiles compareDownloadConfig "previous/1"
  void $ compareEntryFile compareDownloadConfig "previous/entries/0"
  return ()

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = Opt.info (Opt.helper <*> config)
      ( Opt.fullDesc
     <> Opt.progDesc "DynamoDB event store client"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
