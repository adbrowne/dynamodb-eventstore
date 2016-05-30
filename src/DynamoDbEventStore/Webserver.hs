{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}

module DynamoDbEventStore.Webserver(app, positiveInt64Parser, runParser, realRunner) where

import           BasicPrelude
import           Web.Scotty.Trans

import           Control.Arrow             (left)
import           Data.Attoparsec.Text.Lazy
import           Data.Char                 (isDigit)
import           Control.Monad.Reader
import qualified Data.UUID as UUID
import qualified Data.Text.Lazy            as TL
import qualified Data.Text            as T
import qualified Data.Text.Lazy.Encoding   as TL
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Time.Clock (UTCTime)
import           Data.Time.Format
import qualified Data.Time.Clock as Time
import           Data.Aeson
import           Data.Aeson.Encode.Pretty
import           Network.HTTP.Types.Status
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.Feed

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
  fmap (read . fromString) $ many1 (satisfy isDigit) <* endOfInput

textParser :: Parser Text
textParser =
  fmap fromString $ many1 (satisfy (const True)) <* endOfInput

uuidParser :: Parser UUID.UUID
uuidParser = do
  t <- textParser
  case (UUID.fromText t) of Nothing  -> fail "Could not parse UUID"
                            (Just v) -> return v

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

eventStoreActionResultToText :: (MonadIO m, ScottyError e) => ResponseEncoding -> EventStoreActionResult -> ActionT e m ()
eventStoreActionResultToText AtomJsonEncoding (PostEventResult r) = (raw . TL.encodeUtf8 . TL.fromStrict) $ show r
eventStoreActionResultToText AtomJsonEncoding (ReadEventResult (Right (Just r))) = (raw . encodePretty . readEventResultJsonValue) r
eventStoreActionResultToText AtomJsonEncoding (ReadEventResult (Right Nothing)) = (status $ mkStatus 404 (toByteString "Not Found")) >> raw "{}"
eventStoreActionResultToText AtomJsonEncoding (ReadStreamResult (Right xs)) = 
  let 
    streamId = StreamId "todo stream name"
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed' = recordedEventsToFeed baseUri streamId sampleTime
  in raw . encodePretty . jsonFeed . buildFeed' $ xs
eventStoreActionResultToText AtomJsonEncoding (ReadAllResult (Right xs)) = 
  let 
    streamId = StreamId "%24all"
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed' = recordedEventsToFeed baseUri streamId sampleTime
  in raw . encodePretty . jsonFeed . buildFeed' $ xs
   
eventStoreActionResultToText AtomJsonEncoding s = error $ "todo EventStoreActionResult" <> T.unpack (show s)

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

realRunner :: (EventStoreAction -> IO (Either InterpreterError EventStoreActionResult)) -> Process
realRunner mainRunner eventStoreAction = do
  result <- liftIO $ mainRunner eventStoreAction
  case result of (Left err) -> (error500 . TL.fromStrict . show) err
                 (Right a) -> (eventStoreActionResultToText AtomJsonEncoding) a

type Process = forall m. forall e. (MonadIO m, Monad m, ScottyError e) => EventStoreAction -> ActionT e m ()

data EventStartPosition = EventStartHead | EventStartPosition Int64

eventStartPositionParser :: Parser EventStartPosition
eventStartPositionParser =
  let 
    headParser = const EventStartHead <$> string "head" 
    eventNumberParser = EventStartPosition <$> positiveInt64Parser
  in headParser <|> eventNumberParser

eventStartPositionToMaybeInt64 :: Maybe EventStartPosition -> Maybe Int64
eventStartPositionToMaybeInt64 Nothing = Nothing
eventStartPositionToMaybeInt64 (Just EventStartHead) = Nothing
eventStartPositionToMaybeInt64 (Just (EventStartPosition x)) = Just x

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
    startEvent <- parseOptionalParameter "Invalid event number" eventStartPositionParser startEventParameter
    eventCount <- readOptionalParameter "Invalid event count" eventCountParameter
    if (streamId == "$all") then
      process $ ReadAll ReadAllRequest
    else
      toResult' process $ (ReadStream <$> (ReadStreamRequest
            <$> notEmpty streamId
            <*> (eventStartPositionToMaybeInt64 <$> startEvent)
            <*> ((fromMaybe 10) <$> eventCount)
            <*> Right feedDirection))

app :: (MonadIO m, MonadHasTime m, ScottyError e) => Process -> ScottyT e m ()
app process = do
  post "/streams/:streamId" $ do
    streamId <- param "streamId"
    expectedVersion <- parseOptionalHeader "ES-ExpectedVersion" positiveInt64Parser
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
    toResult' process $ (PostEvent <$> (PostEventRequest
          <$> pure streamId
          <*> expectedVersion
          <*> ((\x -> x:|[]) <$> eventEntries)))
  get "/streams/:streamId/:eventNumber" $ do
    streamId <- param "streamId"
    eventNumber <- param "eventNumber"
    toResult' process $ (ReadEvent <$> (ReadEventRequest
          <$> notEmpty streamId
          <*> runParser positiveInt64Parser "Invalid Event Number" eventNumber))
  get "/streams/:streamId" $ readStreamHandler process (param "streamId") Nothing Nothing FeedDirectionBackward
  get "/streams/:streamId/:eventNumber/:count" $ readStreamHandler process (param "streamId") (Just $ param "eventNumber") (Just $ param "count") FeedDirectionBackward
  get "/streams/:streamId/:eventNumber/backward/:count" $ readStreamHandler process (param "streamId") (Just $ param "eventNumber") (Just $ param "count") FeedDirectionBackward
  get "/streams/:streamId/:eventNumber/forward/:count" $ readStreamHandler process (param "streamId") (Just $ param "eventNumber") (Just $ param "count") FeedDirectionForward
  notFound $ status status404
