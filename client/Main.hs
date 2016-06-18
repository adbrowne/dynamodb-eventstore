{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           BasicPrelude
import           Control.Lens
import qualified Data.Text                as T
import           Data.Text.Encoding.Error (lenientDecode)
import qualified Data.Text.Lazy           as TL
import           Data.Text.Lazy.Encoding  (decodeUtf8With)
import           Network.Wreq
import qualified Options.Applicative      as Opt
import           Text.Taggy.Lens
import           Turtle.Prelude

extractPrevious :: Text -> IO ([Text], Response LByteString)
extractPrevious url = do
  print url
  let opts =
        defaults
        & header "Accept" .~ ["application/atom+xml"]
        & auth ?~ basicAuth "admin" "changeit"
  r <- getWith opts (T.unpack url)
  let blah = r ^. (responseBody . to (decodeUtf8With lenientDecode))
  let foo = (toListOf $ html . allNamed (only "link") . attributed (ix "rel" . only "previous") . attr "href") blah
  return (catMaybes foo, r)

followPrevious :: Text -> IO [Response LByteString]
followPrevious startUrl = go [startUrl]
  where
    go [] = return []
    go urls = do
      results <- sequence $ extractPrevious <$> urls
      let nextUrls = join $ fst <$> results
      let responses = snd <$> results
      nextResponses <- go nextUrls
      return $ responses ++ nextResponses

data Config = Config
  { configStartUrl        :: Text,
    configOutputDirectory :: Text }

config :: Opt.Parser Config
config = Config
   <$> (T.pack <$> Opt.strOption (Opt.long "startUrl"
       <> Opt.metavar "STARTURL"
       <> Opt.help "starting url"))
   <*> (T.pack <$> Opt.strOption (Opt.long "outputDirectory"
       <> Opt.metavar "OUTPUTDIRECTORY"
       <> Opt.help "ouput directory for responses"))

outputResponse :: Text -> Text -> (Int, Response LByteString) -> IO ()
outputResponse baseDir responseType (sequenceNumber, response) = do
  let directory = baseDir <> "/" <> responseType <> "/"
  let filename = T.unpack $ directory <> show sequenceNumber
  let fileContent =
        response
        ^. (responseBody
            . to (decodeUtf8With lenientDecode)
            . to TL.toStrict
           )
  mktree (fromString $ T.unpack directory )
  writeTextFile (fromString filename) fileContent

start :: Config -> IO ()
start Config{..} = do
  responses <- followPrevious configStartUrl
  let numberedResponses = zip [0..]  responses
  void $ sequence $ outputResponse configOutputDirectory "previous" <$> numberedResponses

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = Opt.info (Opt.helper <*> config)
      ( Opt.fullDesc
     <> Opt.progDesc "DynamoDB event store client"
     <> Opt.header "DynamoDB Event Store - all your events are belong to us" )
