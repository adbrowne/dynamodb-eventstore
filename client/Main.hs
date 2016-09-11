{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

module Main where

import BasicPrelude hiding (log)
import Control.Concurrent.STM.TQueue
import Control.Lens hiding (children, element)
import Control.Monad.Except
import Control.Monad.State
import qualified Control.Monad.Trans.AWS as AWS
import Data.Aeson
import Data.Aeson.Diff
import Data.Aeson.Encode
import Data.Aeson.Lens
import Data.Algorithm.Diff
import Data.Algorithm.DiffOutput
import Data.Attoparsec.ByteString.Lazy
import qualified Data.ByteString as B
import Data.ByteString.Builder
import Data.Char (isAlphaNum)
import qualified Data.HashMap.Strict as HM
import Data.List.Utils (replace)
import Data.Maybe
import qualified Data.Sequence as Seq
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Text.Encoding.Error (lenientDecode)
import qualified Data.Text.Lazy as TL
import Data.Text.Lazy.Encoding as TL
import qualified Data.UUID (UUID)
import DynamoDbEventStore.EventStoreCommands
import DynamoDbEventStore.AmazonkaImplementation
import qualified DynamoDbEventStore.GlobalFeedWriter
       as GlobalFeedWriter
import qualified DynamoDbEventStore.GlobalFeedItem
       as GlobalFeedItem
import Network.Wreq
import qualified Options.Applicative as Opt
import qualified Prelude as P
import Safe
import System.Directory
import System.FilePath.Posix
import System.IO (stdout)
import System.Random
import Text.Blaze.Renderer.Pretty
import Text.Taggy.DOM
import Text.Taggy.Lens
import Text.Taggy.Renderer
import Turtle.Prelude

getFeedPage :: Text -> IO (Response LByteString)
getFeedPage url = do
    let opts = 
            defaults & header "Accept" .~ ["application/atom+xml"] & auth ?~
            basicAuth "admin" "changeit"
    getWith opts (T.unpack url)

getEntry :: Bool -> Text -> IO (Response LByteString)
getEntry isEmbedTryHarder url = do
    let opts = 
            defaults & header "Accept" .~
            ["application/vnd.eventstore.atom+json"] &
            auth ?~
            basicAuth "admin" "changeit"
    let getUrl = 
            if isEmbedTryHarder
                then T.unpack url <> "?embed=tryharder"
                else T.unpack url
    getWith opts getUrl

getRelLinks :: Text -> LText -> [Text]
getRelLinks relName = 
    catMaybes .
    toListOf
        (html .
         allNamed (only "link") .
         attributed (ix "rel" . only relName) . attr "href")

getBodyAsText :: Response LByteString -> LText
getBodyAsText r = r ^. (responseBody . to (TL.decodeUtf8With lenientDecode))

extractNext :: Text -> IO ([Text], [Text], Response LByteString)
extractNext url = do
    r <- getFeedPage url
    let bodyText = getBodyAsText r
    let feedLinks = getRelLinks "next" bodyText
    let feedEntries = getRelLinks "edit" bodyText
    return (feedLinks, feedEntries, r)

followNext :: Text -> IO [([Text], Response LByteString)]
followNext startUrl = go [startUrl]
  where
    go [] = return []
    go urls = do
        results <- sequence $ extractNext <$> urls
        let nextUrls = 
                join $
                (\(a,_b,_c) -> 
                      a) <$>
                results
        let responses = 
                (\(_a,b,c) -> 
                      (b, c)) <$>
                results
        nextResponses <- go nextUrls
        return $ responses ++ nextResponses

data Config = Config
    { configCommand :: Command
    } 

data Command
    = DownloadGlobalStream DownloadGlobalStreamConfig
    | DownloadFeed DownloadFeedConfig
    | CopyGlobalStream CopyGlobalStreamConfig
    | CompareDownload CompareDownloadConfig
    | InsertData InsertDataConfig
    | SpeedTest 

data DownloadFeedConfig = DownloadFeedConfig
    { downloadFeedConfigOutputDirectory :: Text
    , downloadFeedConfigStartUrl :: Text
    } 

data DownloadGlobalStreamConfig = DownloadGlobalStreamConfig
    { downloadGlobalStreamOutputDirectory :: Text
    , downloadGlobalStreamStartUrl :: Text
    } 

data InsertDataConfig = InsertDataConfig
    { insertDataConfigDestination :: Text
    } 

data CopyGlobalStreamConfig = CopyGlobalStreamConfig
    { copyGlobalStreamConfigStartUrl :: Text
    , copyGlobalStreamConfigDestination :: Text
    } 

data CompareDownloadConfig = CompareDownloadConfig
    { compareDownloadConfigLeftDirectory :: String
    , compareDownloadConfigRightDirectory :: String
    } 

config :: Opt.Parser Config
config = 
    Config <$>
    Opt.subparser
        (Opt.command
             "downloadGlobalStream"
             (Opt.info
                  downloadGlobalStreamOptions
                  (Opt.progDesc "Download global feed moving backward in time")) <>
         Opt.command
             "download-feed"
             (Opt.info
                  downloadFeedOptions
                  (Opt.progDesc "Download feed moving backward in time")) <>
         Opt.command
             "copyGlobalStream"
             (Opt.info
                  copyGlobalStreamOptions
                  (Opt.progDesc "Copy global stream")) <>
         Opt.command
             "compareDownload"
             (Opt.info
                  compareDownloadOptions
                  (Opt.progDesc "Compare downloaded streams")) <>
         Opt.command
             "speedTest"
             (Opt.info
                  speedTestOptions
                  (Opt.progDesc "Test bulk creation of pages")) <>
         Opt.command
             "insertData"
             (Opt.info
                  insertDataOptions
                  (Opt.progDesc "Insert data into event store")))
  where
    downloadGlobalStreamOptions :: Opt.Parser Command
    downloadGlobalStreamOptions = 
        DownloadGlobalStream <$>
        (DownloadGlobalStreamConfig <$>
         (T.pack <$>
          Opt.strOption
              (Opt.long "outputDirectory" <> Opt.metavar "OUTPUTDIRECTORY" <>
               Opt.help "ouput directory for responses")) <*>
         (T.pack <$>
          Opt.strOption
              (Opt.long "startUrl" <> Opt.metavar "STARTURL" <>
               Opt.help "starting url")))
    downloadFeedOptions :: Opt.Parser Command
    downloadFeedOptions = 
        DownloadFeed <$>
        (DownloadFeedConfig <$>
         (T.pack <$>
          Opt.strOption
              (Opt.long "outputDirectory" <> Opt.metavar "OUTPUTDIRECTORY" <>
               Opt.help "ouput directory for responses")) <*>
         (T.pack <$>
          Opt.strOption
              (Opt.long "startUrl" <> Opt.metavar "STARTURL" <>
               Opt.help "starting url")))
    copyGlobalStreamOptions :: Opt.Parser Command
    copyGlobalStreamOptions = 
        CopyGlobalStream <$>
        (CopyGlobalStreamConfig <$>
         (T.pack <$>
          Opt.strOption
              (Opt.long "startUrl" <> Opt.metavar "STARTURL" <>
               Opt.help "starting url")) <*>
         (T.pack <$>
          Opt.strOption
              (Opt.long "destination" <> Opt.metavar "destination" <>
               Opt.help "destination event store - base url")))
    insertDataOptions :: Opt.Parser Command
    insertDataOptions = 
        InsertData <$>
        (InsertDataConfig <$>
         (T.pack <$>
          Opt.strOption
              (Opt.long "destination" <> Opt.metavar "destination" <>
               Opt.help "destination event store - base url")))
    compareDownloadOptions :: Opt.Parser Command
    compareDownloadOptions = 
        CompareDownload <$>
        (CompareDownloadConfig <$>
         Opt.strOption
             (Opt.long "referenceDirectory" <>
              Opt.metavar "REFERENCE_DIRECTORY" <>
              Opt.help "reference directory") <*>
         Opt.strOption
             (Opt.long "testDirectory" <> Opt.metavar "TEST_DIRECTORY" <>
              Opt.help "test directory"))
    speedTestOptions :: Opt.Parser Command
    speedTestOptions = pure SpeedTest

saveResponse :: Text -> Text -> Response LByteString -> IO ()
saveResponse directory filename response = do
    let fileContent = 
            response ^.
            (responseBody . to (decodeUtf8With lenientDecode) . to TL.toStrict)
    mktree (fromString $ T.unpack directory)
    writeTextFile (fromString $ T.unpack filename) fileContent

outputResponse :: Text -> Text -> (Int, ([Text], Response LByteString)) -> IO ()
outputResponse baseDir responseType (sequenceNumber,(_editLinks,response)) = do
    let directory = baseDir <> "/" <> responseType <> "/"
    let filename = directory <> show sequenceNumber <> ".xml"
    saveResponse directory filename response

outputEntry :: Text -> Text -> (Int, Response LByteString) -> IO ()
outputEntry baseDir responseType (sequenceNumber,response) = do
    let directory = baseDir <> "/" <> responseType <> "/entries/"
    let filename = directory <> show sequenceNumber <> ".json"
    saveResponse directory filename response

data EntryData = EntryData
    { entryDataType :: Text
    , entryDataStream :: Text
    , entryDataBody :: Maybe LByteString
    } deriving ((Show))

toEntryData :: Response LByteString -> EntryData
toEntryData r = 
    let body = view responseBody r
    in fromJust $
       do jsonValue <- maybeResult . parse json $ body
          streamId <- preview (key "streamId" . _String) jsonValue
          eventType <- preview (key "eventType" . _String) jsonValue
          let dataBody = 
                  preview
                      (key "content" .
                       key "data" . to (toLazyByteString . encodeToBuilder))
                      jsonValue
          return
              EntryData
              { entryDataType = eventType
              , entryDataStream = streamId
              , entryDataBody = dataBody
              }

putEntry :: Text -> EntryData -> IO ()
putEntry destinationBaseUrl EntryData{..}
                 -- threadDelay 1000000
 = do
    let url = T.unpack $ destinationBaseUrl <> "/streams/" <> entryDataStream
    let body = fromMaybe (fromString "") entryDataBody
    (eventId :: Data.UUID.UUID) <- randomIO
    print $ "Entry: " <> entryDataStream <> " " <> entryDataType
    let opts = 
            defaults & header "ES-EventType" .~ [T.encodeUtf8 entryDataType] &
            header "ES-EventId" .~
            [T.encodeUtf8 $ show eventId] &
            header "Content-Type" .~
            ["application/json"] &
            header "Accept" .~
            ["application/vnd.eventstore.atom+json"]
    _ <- postWith opts url body
    return ()

readXmlFile :: String -> IO Node
readXmlFile filePath = do
    fileContents <- T.strip <$> readTextFile (fromString filePath)
    return $ head $ parseDOM False (TL.fromStrict fileContents)

readJsonFile :: String -> IO Value
readJsonFile filePath = do
    fileContents <- B.readFile (fromString filePath)
    return $ fromJust $ fileContents ^? _Value

normalizeDom :: Node -> Maybe Node
normalizeDom n@(NodeContent _) = Just n
normalizeDom (NodeElement el@Element{eltName = "link",..}) = 
    let eltAttrs' = HM.adjust (const "normalized") "href" eltAttrs
        relValue = HM.lookup "rel" eltAttrs
    in if relValue == Just "metadata"
           then Nothing
           else Just $
                NodeElement
                    el
                    { eltAttrs = eltAttrs'
                    , eltChildren = catMaybes $ normalizeDom <$> eltChildren
                    }
normalizeDom (NodeElement el@Element{eltName = "updated",..}) = 
    Just $
    NodeElement
        el
        { eltChildren = []
        }
normalizeDom (NodeElement el@Element{eltName = "xml",..}) = 
    Just $
    NodeElement
        el
        { eltChildren = catMaybes $ normalizeDom <$> eltChildren
        , eltAttrs = HM.adjust T.toLower "encoding" eltAttrs
        }
normalizeDom (NodeElement el@Element{..}) = 
    Just $
    NodeElement
        el
        { eltChildren = catMaybes $ normalizeDom <$> eltChildren
        }

renderNode :: Node -> Text
renderNode node = T.pack $ renderMarkup $ toMarkup False node

compareFeedFiles :: String -> String -> IO Bool
compareFeedFiles leftFilePath rightFilePath = do
    domLeft <- normalizeDom <$> readXmlFile leftFilePath
    domRight <- normalizeDom <$> readXmlFile rightFilePath
    let areEqual = domLeft == domRight
    print areEqual
    unless areEqual $
        do putStrLn
               ("Left: " <> T.pack leftFilePath <> " Right: " <>
                T.pack rightFilePath)
           let theDiff = 
                   getGroupedDiff
                       (domToTextLines domLeft)
                       (domToTextLines domRight)
           let diffOut = ppDiff theDiff
           putStr $ T.pack diffOut
    return areEqual
  where
    domToTextLines n = P.lines . T.unpack . fromMaybe "" $ renderNode <$> n

normalizeJson :: Value -> Value
normalizeJson = 
    let removeMetadata = over (key "content" . _Object) (HM.delete "metadata")
        replaceDigitWithZero :: Char -> Char
        replaceDigitWithZero a
          | isAlphaNum a = '0'
        replaceDigitWithZero a = a
        zeroOutAlphaNum myPrism = over myPrism (T.map replaceDigitWithZero)
        replaceWithString myPrism = set myPrism "normalized"
    in removeMetadata .
       zeroOutAlphaNum (key "content" . key "eventId" . _String) .
       replaceWithString (key "updated" . _String)

compareEntryFile :: String -> String -> IO Bool
compareEntryFile leftFilePath rightFilePath = do
    domLeft <- normalizeJson <$> readJsonFile leftFilePath
    domRight <- normalizeJson <$> readJsonFile rightFilePath
    let areEqual = domLeft == domRight
    print areEqual
    unless areEqual $
        do putStrLn
               ("Left: " <> T.pack leftFilePath <> " Right: " <>
                T.pack rightFilePath)
           putStrLn ("Left DOM:" :: Text)
           print domLeft
           putStrLn ("Right DOM:" :: Text)
           print domRight
           putStrLn "Diff:"
           print $ diff domLeft domRight
    return areEqual

listDirectoryTree :: FilePath -> IO [FilePath]
listDirectoryTree path = do
    let prependPath = combine path
    directoryChildren <- filterSpecialEntries <$> getDirectoryContents path
    childrenWithIsDir <- 
        mapM
            (\a -> 
                  doesDirectoryExist (prependPath a) >>=
                  (\isDir -> 
                        return (isDir, a)))
            directoryChildren
    let (files',directories') = partition ((== False) . fst) childrenWithIsDir
    let files = prependPath . snd <$> files'
    let directories = snd <$> directories'
    subFiles <- sequence $ listDirectoryTree . prependPath <$> directories
    return $ files ++ join subFiles
  where
    filterSpecialEntries :: [FilePath] -> [FilePath]
    filterSpecialEntries = filter ((/= Just '.') . headMay)

compareFile :: CompareDownloadConfig -> FilePath -> IO Bool
compareFile CompareDownloadConfig{..} leftFilePath = do
    let rightFilePath = 
            replace
                compareDownloadConfigLeftDirectory
                compareDownloadConfigRightDirectory
                leftFilePath
    let extension = takeExtension leftFilePath
    case extension of
        ".json" -> compareEntryFile leftFilePath rightFilePath
        ".xml" -> compareFeedFiles leftFilePath rightFilePath
        _ -> error $ "compareFile with unkown extension" <> extension

makeTestEntry :: Int -> EntryData
makeTestEntry n = 
    EntryData
    { entryDataType = "MyEntryType" <> show n
    , entryDataStream = "mystream"
    , entryDataBody = Just . TL.encodeUtf8 . TL.fromStrict $
      "{ \"a\":" <> show n <> "}"
    }

nullMetrics :: MetricLogs
nullMetrics = 
    MetricLogs
    { metricLogsReadItem = doNothingPair
    , metricLogsWriteItem = doNothingPair
    , metricLogsUpdateItem = doNothingPair
    , metricLogsQuery = doNothingPair
    , metricLogsScan = doNothingPair
    }
  where
    doNothingPair = 
        MetricLogsPair
        { metricLogsPairCount = return ()
        , metricLogsPairTimeMs = const $ return ()
        }

start :: Config -> IO ()
start Config{configCommand = DownloadGlobalStream DownloadGlobalStreamConfig{..}} = do
    responses <- followNext downloadGlobalStreamStartUrl
    let numberedResponses = zip [0 ..] responses
    entryBodies <- sequence $ getEntry False <$> join (fst <$> responses)
    let numberedEntries = reverse $ drop 7 $ reverse $ zip [0 ..] entryBodies -- ignore the initial entries
    void $
        sequence $
        outputResponse downloadGlobalStreamOutputDirectory "next" <$>
        numberedResponses
    void $
        sequence $
        outputEntry downloadGlobalStreamOutputDirectory "next" <$>
        numberedEntries
start Config{configCommand = DownloadFeed DownloadFeedConfig{..}} = do
    responses <- followNext downloadFeedConfigStartUrl
    let numberedResponses = zip [0 ..] responses
    void $
        sequence $
        outputResponse downloadFeedConfigOutputDirectory "next" <$>
        numberedResponses
start Config{configCommand = CopyGlobalStream CopyGlobalStreamConfig{..}} = do
    responses <- followNext copyGlobalStreamConfigStartUrl
    entryBodies <- sequence $ getEntry True <$> join (fst <$> responses)
    let entryData = reverse $ toEntryData <$> entryBodies
    sequence_ $ putEntry copyGlobalStreamConfigDestination <$> entryData
    return ()
start Config{configCommand = InsertData InsertDataConfig{..}} = do
    let testEntries = makeTestEntry <$> [0 .. 100]
    sequence_ $ putEntry insertDataConfigDestination <$> testEntries
    return ()
start Config{configCommand = CompareDownload compareDownloadConfig} = do
    files <- 
        listDirectoryTree $
        fromString $ compareDownloadConfigLeftDirectory compareDownloadConfig
    sequence_ $ compareFile compareDownloadConfig <$> files
start Config{configCommand = SpeedTest} = do
    logger <- liftIO $ AWS.newLogger AWS.Error System.IO.stdout
    awsEnv <- set AWS.envLogger logger <$> AWS.newEnv AWS.Sydney AWS.Discover
    thisCompletePageQueue <- newTQueueIO
    let tableName = "estest2"
    let runtimeEnvironment = 
            RuntimeEnvironment
            { _runtimeEnvironmentMetricLogs = nullMetrics
            , _runtimeEnvironmentCompletePageQueue = thisCompletePageQueue
            , _runtimeEnvironmentAmazonkaEnv = awsEnv
            , _runtimeEnvironmentTableName = tableName
            }
    result <- runDynamoCloud runtimeEnvironment writePages
    print result
    return ()

writePages :: MyAwsM ()
writePages = do
    _ <- 
        runExceptT $
        evalStateT
            (mapM_ go [0 .. 1000])
            GlobalFeedWriter.emptyGlobalFeedWriterState
    return ()
  where
    testFeedEntry = 
        FeedEntry
        { feedEntryStream = StreamId "Andrew"
        , feedEntryNumber = 0
        , feedEntryCount = 1
        }
    feedEntries = Seq.fromList $ replicate 1000 testFeedEntry
    go
        :: Int64
        -> (StateT GlobalFeedWriter.GlobalFeedWriterState (ExceptT GlobalFeedWriter.EventStoreActionError MyAwsM)) DynamoWriteResult
    go pageNumber = do
        log Debug ("Writing page" <> show pageNumber)
        GlobalFeedItem.writeGlobalFeedItem
            GlobalFeedItem.GlobalFeedItem
            { globalFeedItemPageKey = PageKey pageNumber
            , globalFeedItemFeedEntries = feedEntries
            , globalFeedItemPageStatus = GlobalFeedItem.PageStatusComplete
            , globalFeedItemVersion = 0
            }

main :: IO ()
main = Opt.execParser opts >>= start
  where
    opts = 
        Opt.info
            (Opt.helper <*> config)
            (Opt.fullDesc <> Opt.progDesc "DynamoDB event store client" <>
             Opt.header
                 "DynamoDB Event Store - all your events are belong to us")
