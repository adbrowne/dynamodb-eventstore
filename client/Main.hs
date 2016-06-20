{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           BasicPrelude
import           Control.Lens                    hiding (children, element)
import           Data.Aeson
import           Data.Aeson.Encode
import           Data.Aeson.Lens
import           Data.Attoparsec.ByteString.Lazy
import           Data.ByteString.Builder
import           Data.Maybe
import qualified Data.Text                       as T
import           Data.Text.Encoding.Error        (lenientDecode)
import qualified Data.Text.Lazy                  as TL
import           Data.Text.Lazy.Encoding         (decodeUtf8With)
import           Network.Wreq
import qualified Options.Applicative             as Opt
import           Text.Taggy.Lens
import           Turtle.Prelude

getFeedPage :: Text -> IO (Response LByteString)
getFeedPage url = do
  let opts =
        defaults
        & header "Accept" .~ ["application/atom+xml"]
        & auth ?~ basicAuth "admin" "changeit"
  getWith opts (T.unpack url)

getEntry :: Text -> IO (Response LByteString)
getEntry url = do
  let opts =
        defaults
        & header "Accept" .~ ["application/vnd.eventstore.atom+json"]
        & auth ?~ basicAuth "admin" "changeit"
  getWith opts (T.unpack url <> "?embed=tryharder")

extractPrevious :: Text -> IO ([Text], [Text], Response LByteString)
extractPrevious url = do
  r <- getFeedPage url
  let bodyText = r ^. (responseBody . to (decodeUtf8With lenientDecode))
  let feedLinks = (toListOf $ html . allNamed (only "link") . attributed (ix "rel" . only "previous") . attr "href") bodyText
  let feedEntries = (toListOf $ html . allNamed (only "link") . attributed (ix "rel" . only "edit") . attr "href") bodyText
  return (catMaybes feedLinks, catMaybes feedEntries, r)

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

data DownloadGlobalStreamConfig = DownloadGlobalStreamConfig {
  downloadGlobalStreamOutputDirectory :: Text,
  downloadGlobalStreamStartUrl        :: Text }

data CopyGlobalStreamConfig = CopyGlobalStreamConfig {
  copyGlobalStreamConfigStartUrl    :: Text,
  copyGlobalStreamConfigDestination :: Text }

config :: Opt.Parser Config
config = Config
   <$> Opt.subparser
         (Opt.command "downloadGlobalStream" (Opt.info downloadGlobalStreamOptions (Opt.progDesc "Download global stream moving forward in time") )
         <>
          Opt.command "copyGlobalStream" (Opt.info copyGlobalStreamOptions (Opt.progDesc "Copy global stream"))
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
    let dataBody = (toLazyByteString . encodeToBuilder) <$> preview (key "content" . key "data") jsonValue
    return EntryData {
      entryDataType = eventType
      , entryDataStream = streamId
      , entryDataBody = dataBody
    }

start :: Config -> IO ()
start Config { configCommand = DownloadGlobalStream DownloadGlobalStreamConfig{..}} = do
  responses <- followPrevious downloadGlobalStreamStartUrl
  let numberedResponses = zip [0..]  responses
  entryBodies <- sequence $ getEntry <$> join (fst <$> responses)
  let numberedEntries = zip [0..] entryBodies
  void $ sequence $ outputResponse downloadGlobalStreamOutputDirectory "previous" <$> numberedResponses
  void $ sequence $ outputEntry downloadGlobalStreamOutputDirectory "previous" <$> numberedEntries
start Config { configCommand = CopyGlobalStream CopyGlobalStreamConfig{..} } = do
  responses <- followPrevious copyGlobalStreamConfigStartUrl
  entryBodies <- sequence $ getEntry <$> join (fst <$> responses)
  let entryData = toEntryData <$> entryBodies
  sequence_ $ print <$> entryData
  return ()

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = Opt.info (Opt.helper <*> config)
      ( Opt.fullDesc
     <> Opt.progDesc "DynamoDB event store client"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
