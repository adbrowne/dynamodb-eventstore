{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           BasicPrelude
import           Control.Lens             hiding (children, element)
import qualified Data.Text                as T
import           Data.Text.Encoding.Error (lenientDecode)
import qualified Data.Text.Lazy           as TL
import           Data.Text.Lazy.Encoding  (decodeUtf8With)
import           Network.Wreq
import qualified Options.Applicative      as Opt
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
  getWith opts (T.unpack url)

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
    | CopyGlobalStream

data DownloadGlobalStreamConfig = DownloadGlobalStreamConfig {
  downloadGlobalStreamOutputDirectory :: Text,
  downloadGlobalStreamStartUrl        :: Text }

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
     copyGlobalStreamOptions = pure CopyGlobalStream

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

start :: Config -> IO ()
start Config { configCommand = DownloadGlobalStream DownloadGlobalStreamConfig{..}} = do
  responses <- followPrevious downloadGlobalStreamStartUrl
  let numberedResponses = zip [0..]  responses
  entryBodies <- sequence $ getEntry <$> join (fst <$> responses)
  let numberedEntries = zip [0..] entryBodies
  void $ sequence $ outputResponse downloadGlobalStreamOutputDirectory "previous" <$> numberedResponses
  void $ sequence $ outputEntry downloadGlobalStreamOutputDirectory "previous" <$> numberedEntries
start Config { configCommand = CopyGlobalStream } = return () -- todo
main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = Opt.info (Opt.helper <*> config)
      ( Opt.fullDesc
     <> Opt.progDesc "DynamoDB event store client"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
