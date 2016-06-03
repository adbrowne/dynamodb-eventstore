{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module DynamoDbEventStore.Webserver(
    app
  , positiveInt64Parser
  , runParser
  , realRunner
  , EventStoreActionRunner(..)
  , parseGlobalFeedPosition
  , globalFeedPositionToText
  ) where

import           BasicPrelude
import           Web.Scotty.Trans

import           Control.Arrow                          (left)
import           Control.Monad.Reader
import           Data.Aeson
import           Data.Aeson.Encode.Pretty
import           Data.Attoparsec.Text.Lazy
import           Data.Char                              (isDigit)
import           Data.List.NonEmpty                     (NonEmpty (..))
import qualified Data.Text.Lazy                         as TL
import qualified Data.Text.Lazy.Encoding                as TL
import           Data.Time.Clock                        (UTCTime)
import qualified Data.Time.Clock                        as Time
import           Data.Time.Format
import qualified Data.UUID                              as UUID
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.Feed
import           Network.HTTP.Types.Status

data ExpectedVersion = ExpectedVersion Int
  deriving (Show)

toByteString :: LText -> ByteString
toByteString = encodeUtf8 . TL.toStrict

error400 :: (MonadIO m, ScottyError e) => LText -> ActionT e m ()
error400 err = status $ mkStatus 400 (toByteString err)

error500 :: (MonadIO m, ScottyError e) => LText -> ActionT e m ()
error500 err = status $ mkStatus 500 (toByteString err)

runParser :: Parser a -> e -> LText -> Either e a
runParser p e = left (const e) . eitherResult . parse p

headerError :: LText -> LText -> LText
headerError headerName message =
  mconcat [headerName, " header ", message]

maybeToEither :: a -> Maybe b ->Either a b
maybeToEither a Nothing = Left a
maybeToEither _ (Just a) = Right a

globalFeedPositionToText :: GlobalFeedPosition -> Text
globalFeedPositionToText GlobalFeedPosition{..} = show globalFeedPositionPage <> "-" <> show globalFeedPositionOffset

globalFeedPositionParser :: Parser GlobalFeedPosition
globalFeedPositionParser =
  GlobalFeedPosition
    <$> positiveInt64Parser
    <*> (string "-" *> positiveIntParser)

parseGlobalFeedPosition :: Text -> Maybe GlobalFeedPosition
parseGlobalFeedPosition =
  maybeResult . parse (globalFeedPositionParser <* endOfInput) . TL.fromStrict

parseMandatoryHeader :: (MonadIO m, ScottyError e) => LText -> Parser a -> ActionT e m (Either LText a)
parseMandatoryHeader headerName parser = do
  headerText <- header headerName
  return $
    maybeToEither missingErrorText headerText
    >>= runParser parser parseFailErrorText
  where
    missingErrorText = headerError headerName "is missing"
    parseFailErrorText = headerError headerName "in wrong format"

parseOptionalHeader :: (MonadIO m, ScottyError e) => LText -> Parser a -> ActionT e m (Either LText (Maybe a))
parseOptionalHeader headerName parser = do
  headerValue <- header headerName
  case headerValue of Nothing -> return $ Right Nothing
                      Just headerText -> return $ Just <$> (Right headerText >>= runParser parser parseFailErrorText)
  where
    parseFailErrorText = headerError headerName "in wrong format"

maxInt64 :: Integer
maxInt64 = toInteger (maxBound :: Int64)

positiveIntegerParser :: Parser Integer
positiveIntegerParser =
  (read . fromString) <$> many1 (satisfy isDigit)

textParser :: Parser Text
textParser =
  fmap fromString $ many1 (satisfy (const True)) <* endOfInput

uuidParser :: Parser UUID.UUID
uuidParser = do
  t <- textParser
  case UUID.fromText t of Nothing  -> fail "Could not parse UUID"
                          (Just v) -> return v

positiveIntParser :: Parser Int
positiveIntParser =
  filterInt =<< positiveIntegerParser
  where
    filterInt :: Integer -> Parser Int
    filterInt a
     | a <= toInteger (maxBound :: Int) = return (fromInteger a)
     | otherwise = fail "too large"

positiveInt64Parser :: Parser Int64
positiveInt64Parser =
  filterInt64 =<< positiveIntegerParser
  where
    filterInt64 :: Integer -> Parser Int64
    filterInt64 a
     | a <= maxInt64 = return (fromInteger a)
     | otherwise = fail "too large"

readEventResultJsonValue :: RecordedEvent -> Value
readEventResultJsonValue recordedEvent =
  jsonEntry $ recordedEventToFeedEntry baseUri recordedEvent

baseUri :: Text
baseUri = "http://localhost:2114"

eventStorePostResultToText :: (MonadIO m, ScottyError e) => ResponseEncoding -> PostEventResult -> ActionT e m ()
eventStorePostResultToText AtomJsonEncoding (PostEventResult r) = (raw . TL.encodeUtf8 . TL.fromStrict) $ show r

notFoundResponse :: (MonadIO m, ScottyError e) => ActionT e m ()
notFoundResponse = status (mkStatus 404 (toByteString "Not Found")) >> raw "{}"

eventStoreReadEventResultToText :: (MonadIO m, ScottyError e) => ResponseEncoding -> ReadEventResult -> ActionT e m ()
eventStoreReadEventResultToText AtomJsonEncoding (ReadEventResult (Left err)) = (error500 . TL.fromStrict . show) err
eventStoreReadEventResultToText AtomJsonEncoding (ReadEventResult (Right (Just r))) =(raw . encodePretty . readEventResultJsonValue) r
eventStoreReadEventResultToText AtomJsonEncoding (ReadEventResult (Right Nothing)) = notFoundResponse

eventStoreReadStreamResultToText :: (MonadIO m, ScottyError e) => StreamId -> ResponseEncoding -> ReadStreamResult -> ActionT e m ()
eventStoreReadStreamResultToText _ AtomJsonEncoding (ReadStreamResult (Left err)) = (error500 . TL.fromStrict . show) err
eventStoreReadStreamResultToText _streamId AtomJsonEncoding (ReadStreamResult (Right Nothing)) = notFoundResponse
eventStoreReadStreamResultToText streamId AtomJsonEncoding (ReadStreamResult (Right (Just streamResult))) =
  let
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed' = streamResultsToFeed baseUri streamId sampleTime
  in raw . encodePretty . jsonFeed . buildFeed' $ streamResult

eventStoreReadAllResultToText :: (MonadIO m, ScottyError e) => ResponseEncoding -> ReadAllResult -> ActionT e m ()
eventStoreReadAllResultToText AtomJsonEncoding (ReadAllResult (Left err)) = (error500 . TL.fromStrict . show) err
eventStoreReadAllResultToText AtomJsonEncoding (ReadAllResult (Right xs)) =
  let
    streamId = StreamId "%24all"
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed' = streamResultsToFeed baseUri streamId sampleTime
  in raw . encodePretty . jsonFeed . buildFeed' $ StreamResult {
    streamResultEvents = xs,
    streamResultLast = Nothing,
    streamResultFirst = Nothing,
    streamResultNext = Nothing,
    streamResultPrevious = Nothing
  }

data ResponseEncoding = AtomJsonEncoding

notEmpty :: Text -> Either LText Text
notEmpty "" = Left "streamId required"
notEmpty t = Right t

class MonadHasTime m where
  getCurrentTime :: m UTCTime

instance MonadHasTime IO where
  getCurrentTime = Time.getCurrentTime

instance (Monad m) => MonadHasTime (ReaderT UTCTime m) where
  getCurrentTime = ask

data EventStoreActionRunner = EventStoreActionRunner {
    eventStoreActionRunnerPostEvent  :: PostEventRequest -> IO (Either InterpreterError PostEventResult)
  , eventStoreActionRunnerReadStream :: ReadStreamRequest -> IO (Either InterpreterError ReadStreamResult)
  , eventStoreActionRunnerReadAll    :: ReadAllRequest -> IO (Either InterpreterError ReadAllResult)
  , eventStoreActionRunnerReadEvent  :: ReadEventRequest -> IO (Either InterpreterError ReadEventResult)
}

realRunner :: EventStoreActionRunner -> Process
realRunner mainRunner (PostEvent postEventRequest) = do
  result <- liftIO $ eventStoreActionRunnerPostEvent mainRunner postEventRequest
  case result of (Left err) -> (error500 . TL.fromStrict . show) err
                 (Right a) -> eventStorePostResultToText AtomJsonEncoding a
realRunner mainRunner (ReadEvent readEventRequest) = do
  result <- liftIO $ eventStoreActionRunnerReadEvent mainRunner readEventRequest
  case result of (Left err) -> (error500 . TL.fromStrict . show) err
                 (Right a) -> eventStoreReadEventResultToText AtomJsonEncoding a
realRunner mainRunner (ReadStream readStreamRequest@ReadStreamRequest{..}) = do
  result <- liftIO $ eventStoreActionRunnerReadStream mainRunner readStreamRequest
  case result of (Left err) -> (error500 . TL.fromStrict . show) err
                 (Right a) -> eventStoreReadStreamResultToText rsrStreamId AtomJsonEncoding a
realRunner mainRunner (ReadAll readAllRequest) = do
  result <- liftIO $ eventStoreActionRunnerReadAll mainRunner readAllRequest
  case result of (Left err) -> (error500 . TL.fromStrict . show) err
                 (Right a) -> eventStoreReadAllResultToText AtomJsonEncoding a

type Process = forall m. forall e. (MonadIO m, Monad m, ScottyError e) => EventStoreAction -> ActionT e m ()

globalFeedStartPositionParser :: Parser GlobalStartPosition
globalFeedStartPositionParser =
  let
    headParser = (const GlobalStartHead <$> string "head")
    eventNumberParser = GlobalStartPosition <$> globalFeedPositionParser
  in (headParser <|> eventNumberParser) <* endOfInput

eventStartPositionParser :: Parser EventStartPosition
eventStartPositionParser =
  let
    headParser = (const EventStartHead <$> string "head")
    eventNumberParser = EventStartPosition <$> positiveInt64Parser
  in (headParser <|> eventNumberParser) <* endOfInput

eventStartPositionToMaybeInt64 :: Maybe EventStartPosition -> Maybe Int64
eventStartPositionToMaybeInt64 Nothing = Nothing
eventStartPositionToMaybeInt64 (Just EventStartHead) = Nothing
eventStartPositionToMaybeInt64 (Just (EventStartPosition x)) = Just x

globalStartPositionToMaybeInt64 :: Maybe GlobalStartPosition -> Maybe GlobalFeedPosition
globalStartPositionToMaybeInt64 Nothing = Nothing
globalStartPositionToMaybeInt64 (Just GlobalStartHead) = Nothing
globalStartPositionToMaybeInt64 (Just (GlobalStartPosition x)) = Just x

readOptionalParameter :: (Read a, Monad m, ScottyError e) => LText -> Maybe (ActionT e m Text) -> ActionT e m (Either LText (Maybe a))
readOptionalParameter errorMsg parameter =
  let parseValue t = maybe (Left errorMsg) (Right . Just) (readMay t)
  in maybe (return $ Right Nothing) (fmap parseValue) parameter

parseOptionalParameter :: (Monad m, ScottyError e) => LText -> Parser a -> Maybe (ActionT e m Text) -> ActionT e m (Either LText (Maybe a))
parseOptionalParameter errorMsg parser parameter =
  let parseValue t = Just <$> runParser parser errorMsg (TL.fromStrict t)
  in maybe (return $ Right Nothing) (fmap parseValue) parameter

toResult' :: (MonadIO m, Monad m, ScottyError e) => Process -> Either LText EventStoreAction -> ActionT e m ()
toResult' _ (Left err) = error400 err
toResult' process (Right action) = process action

readStreamHandler :: (MonadIO m, ScottyError e) => Process -> ActionT e m Text -> Maybe (ActionT e m Text) -> Maybe (ActionT e m Text) -> FeedDirection -> ActionT e m ()
readStreamHandler process streamIdAction startEventParameter eventCountParameter feedDirection = do
    streamId <- streamIdAction
    eventCount <- readOptionalParameter "Invalid event count" eventCountParameter
    if streamId == "$all" || streamId == "%24all" then do
      startPosition <- parseOptionalParameter "Invalid global position" globalFeedStartPositionParser startEventParameter
      toResult' process (ReadAll <$> (ReadAllRequest
        <$> (globalStartPositionToMaybeInt64 <$> startPosition)
        <*> (fromMaybe 20 <$> eventCount)
        <*> Right feedDirection))
    else do
      startEvent <- parseOptionalParameter "Invalid event number" eventStartPositionParser startEventParameter
      toResult' process (ReadStream <$> (ReadStreamRequest
            <$> (StreamId <$> notEmpty streamId)
            <*> (eventStartPositionToMaybeInt64 <$> startEvent)
            <*> (fromMaybe 20 <$> eventCount)
            <*> Right feedDirection))

app :: (MonadIO m, MonadHasTime m, ScottyError e) => Process -> ScottyT e m ()
app process = do
  post "/streams/:streamId" $ do
    streamId <- param "streamId"
    expectedVersion <- parseOptionalHeader "ES-ExpectedVersion" (positiveInt64Parser <* endOfInput)
    eventType <- parseMandatoryHeader "ES-EventType" textParser
    eventData <- body
    eventTime <- lift getCurrentTime
    eventId <- parseMandatoryHeader "ES-EventId" uuidParser
    let eventEntries =
          EventEntry
          <$> pure eventData
          <*> (EventType <$> eventType)
          <*> (EventId <$> eventId)
          <*> pure (EventTime eventTime)
          <*> pure True
    toResult' process (PostEvent <$> (PostEventRequest
          <$> pure streamId
          <*> expectedVersion
          <*> ((\x -> x:|[]) <$> eventEntries)))
  get "/streams/:streamId/:eventNumber" $ do
    streamId <- param "streamId"
    eventNumber <- param "eventNumber"
    toResult' process (ReadEvent <$> (ReadEventRequest
          <$> notEmpty streamId
          <*> runParser (positiveInt64Parser <* endOfInput) "Invalid Event Number" eventNumber))
  get "/streams/:streamId" $ readStreamHandler process (param "streamId") Nothing Nothing FeedDirectionBackward
  get "/streams/:streamId/:eventNumber/:count" $ readStreamHandler process (param "streamId") (Just $ param "eventNumber") (Just $ param "count") FeedDirectionBackward
  get "/streams/:streamId/:eventNumber/backward/:count" $ readStreamHandler process (param "streamId") (Just $ param "eventNumber") (Just $ param "count") FeedDirectionBackward
  get "/streams/:streamId/:eventNumber/forward/:count" $ readStreamHandler process (param "streamId") (Just $ param "eventNumber") (Just $ param "count") FeedDirectionForward
  notFound $ status status404
