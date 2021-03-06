{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
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
  , knownJsonKeyOrder
  ) where

import           BasicPrelude
import           Web.Scotty.Trans

import           Control.Arrow                          (left)
import           Control.Monad.Except
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
import           DynamoDbEventStore
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.Paging
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.Types
import           DynamoDbEventStore.Feed
import           Network.HTTP.Types.Status
import           Text.Blaze.Renderer.Text

data ExpectedVersion = ExpectedVersion Int
  deriving (Show)

toByteString :: LText -> ByteString
toByteString = encodeUtf8 . TL.toStrict

error400 :: (MonadIO m) => LText -> ActionT e m ()
error400 err = status $ mkStatus 400 (toByteString err)

error500 :: (MonadIO m) => LText -> ActionT e m ()
error500 err = status $ mkStatus 500 (toByteString err)

runParser :: Parser a -> e -> LText -> Either e a
runParser p e = left (const e) . eitherResult . parse p

headerError :: LText -> LText -> LText
headerError headerName message =
  mconcat [headerName, " header ", message]

maybeToEither :: a -> Maybe b ->Either a b
maybeToEither a Nothing = Left a
maybeToEither _ (Just a) = Right a

globalFeedPositionParser :: Parser GlobalFeedPosition
globalFeedPositionParser =
  GlobalFeedPosition
    <$> pageKeyParser
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

acceptHeaderToIsJsonParser :: Parser Bool
acceptHeaderToIsJsonParser =
  let
    jsonParser = asciiCI "application/json" >> return True
    binaryParser = asciiCI "application/octet-stream" >> return False
  in jsonParser <|> binaryParser <|> fail "unrecognized content type"

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

pageKeyParser :: Parser PageKey
pageKeyParser = PageKey <$> positiveInt64Parser

readEventResultJsonValue :: Text -> RecordedEvent -> Value
readEventResultJsonValue baseUri recordedEvent =
  jsonEntry $ recordedEventToFeedEntry baseUri recordedEvent

eventStorePostResultToText :: (MonadIO m) => ResponseEncoding -> PostEventResult -> ActionT e m ()
eventStorePostResultToText _ (PostEventResult r) = (raw . TL.encodeUtf8 . TL.fromStrict) $ tshow r

notFoundResponse :: (MonadIO m, ScottyError e) => ActionT e m ()
notFoundResponse = status (mkStatus 404 (toByteString "Not Found")) >> raw "{}"

knownJsonKeyOrder :: [Text]
knownJsonKeyOrder = [
  "title"
  , "id"
  , "updated"
  , "author"
  , "summary"
  , "content"
  , "links"
  , "eventStreamId"
  , "eventNumber"
  , "eventType"
  , "eventId"
  , "data"
  , "metadata"
                    ]

encodeJson :: ToJSON a => a -> LByteString
encodeJson = encodePretty' defConfig {
  confIndent = Spaces 2
  , confCompare = keyOrder knownJsonKeyOrder }

eventStoreReadEventResultToText :: (MonadIO m, ScottyError e) => Text -> ResponseEncoding -> ReadEventResult -> ActionT e m ()
eventStoreReadEventResultToText _ _ (ReadEventResult (Left err)) = (error500 . TL.fromStrict . tshow) err
eventStoreReadEventResultToText baseUri AtomJsonEncoding (ReadEventResult (Right (Just r))) = (raw . encodeJson . readEventResultJsonValue baseUri) r
eventStoreReadEventResultToText baseUri AtomXmlEncoding (ReadEventResult (Right (Just r))) = (raw . encodeJson . readEventResultJsonValue baseUri) r -- todo this isn't right
eventStoreReadEventResultToText _ _ (ReadEventResult (Right Nothing)) = notFoundResponse

encodeFeed :: (MonadIO m) => ResponseEncoding -> Feed -> ActionT e m ()
encodeFeed AtomJsonEncoding = raw . encodeJson . jsonFeed
encodeFeed AtomXmlEncoding  = raw . TL.encodeUtf8 . ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" <>) . renderMarkup . xmlFeed

eventStoreReadStreamResultToText :: (MonadIO m, ScottyError e) => Text -> StreamId -> ResponseEncoding -> ReadStreamResult -> ActionT e m ()
eventStoreReadStreamResultToText _ _ _ (ReadStreamResult (Left err)) = (error500 . TL.fromStrict . tshow) err
eventStoreReadStreamResultToText _ _streamId _ (ReadStreamResult (Right Nothing)) = notFoundResponse
eventStoreReadStreamResultToText baseUri streamId encoding (ReadStreamResult (Right (Just streamResult))) =
  let
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed' = streamResultsToFeed baseUri streamId sampleTime
  in encodeFeed encoding . buildFeed' $ streamResult

eventStoreReadAllResultToText :: (MonadIO m) => Text -> ResponseEncoding -> ReadAllResult -> ActionT e m ()
eventStoreReadAllResultToText _ _ (ReadAllResult (Left err)) = (error500 . TL.fromStrict . tshow) err
eventStoreReadAllResultToText baseUri encoding (ReadAllResult (Right globalStreamResult)) =
  let
    streamId = StreamId "%24all"
    sampleTime = parseTimeOrError True defaultTimeLocale rfc822DateFormat "Sun, 08 May 2016 12:49:41 +0000" -- todo
    buildFeed' = globalStreamResultsToFeed baseUri streamId sampleTime
  in encodeFeed encoding . buildFeed' $ globalStreamResult

data ResponseEncoding = AtomJsonEncoding | AtomXmlEncoding

notEmpty :: Text -> Either LText Text
notEmpty "" = Left "streamId required"
notEmpty t = Right t

class MonadHasTime m where
  getCurrentTime :: m UTCTime

instance MonadHasTime IO where
  getCurrentTime = Time.getCurrentTime

instance (Monad m) => MonadHasTime (ReaderT UTCTime m) where
  getCurrentTime = ask

data WebError =
  UnknownAcceptValue
  | WebErrorInterpreter EventStoreError
  deriving Show

data EventStoreActionRunner = EventStoreActionRunner {
    eventStoreActionRunnerPostEvent  :: PostEventRequest -> IO PostEventResult
  , eventStoreActionRunnerReadStream :: ReadStreamRequest -> IO ReadStreamResult
  , eventStoreActionRunnerReadAll    :: ReadAllRequest -> IO ReadAllResult
  , eventStoreActionRunnerReadEvent  :: ReadEventRequest -> IO ReadEventResult
}

getEncoding :: forall m. forall e. (Monad m, ScottyError e) => ExceptT WebError (ActionT e m) ResponseEncoding
getEncoding = do
  accept <- lift $ header "Accept"
  case accept of (Just "application/vnd.eventstore.atom+json") -> return AtomJsonEncoding
                 (Just "application/atom+xml")                   -> return AtomXmlEncoding
                 _                                              -> throwError UnknownAcceptValue

runActionWithEncodedResponse :: (MonadIO m, ScottyError e) => IO r -> (ResponseEncoding -> r -> ActionT e m ()) -> ActionT e m ()
runActionWithEncodedResponse runAction processResponse = runExceptT (do
  encoding <- getEncoding
  result <- liftAction runAction
  return (encoding, result)) >>= \case
    (Left err) -> (error500 . TL.fromStrict . tshow) err
    (Right (encoding, a)) -> processResponse encoding a
  where
    liftAction :: (MonadIO m, ScottyError e) => IO r -> ExceptT WebError (ActionT e m) r
    liftAction f = do
      r <- liftIO f
      return r

realRunner :: Text -> EventStoreActionRunner -> Process
realRunner _baseUri mainRunner (PostEvent postEventRequest) =
  runActionWithEncodedResponse
    (eventStoreActionRunnerPostEvent mainRunner postEventRequest)
    eventStorePostResultToText
realRunner baseUri mainRunner (ReadEvent readEventRequest) =
  runActionWithEncodedResponse
    (eventStoreActionRunnerReadEvent mainRunner readEventRequest)
    (eventStoreReadEventResultToText baseUri)
realRunner baseUri mainRunner (ReadStream readStreamRequest@ReadStreamRequest{..}) =
  runActionWithEncodedResponse
    (eventStoreActionRunnerReadStream mainRunner readStreamRequest)
    (eventStoreReadStreamResultToText baseUri rsrStreamId)
realRunner baseUri mainRunner (ReadAll readAllRequest) =
  runActionWithEncodedResponse
    (eventStoreActionRunnerReadAll mainRunner readAllRequest)
    (eventStoreReadAllResultToText baseUri)

type Process = forall m. forall e. (MonadIO m, ScottyError e) => EventStoreAction -> ActionT e m ()

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

toResult' :: (MonadIO m, ScottyError e) => Process -> Either LText EventStoreAction -> ActionT e m ()
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

postEventHandler :: (MonadIO m, MonadHasTime m, ScottyError e) => Process -> ActionT e m ()
postEventHandler process = do
  streamId <- param "streamId"
  expectedVersion <- parseOptionalHeader "ES-ExpectedVersion" (positiveInt64Parser <* endOfInput)
  eventType <- parseMandatoryHeader "ES-EventType" textParser
  eventData <- body
  eventTime <- lift getCurrentTime
  eventId <- parseMandatoryHeader "ES-EventId" uuidParser
  isJson <- parseMandatoryHeader "Content-Type" acceptHeaderToIsJsonParser
  let eventEntries =
        EventEntry
        <$> pure eventData
        <*> (EventType <$> eventType)
        <*> (EventId <$> eventId)
        <*> pure (EventTime eventTime)
        <*> isJson
  toResult' process (PostEvent <$> (PostEventRequest
        <$> pure streamId
        <*> expectedVersion
        <*> ((\x -> x:|[]) <$> eventEntries)))
app :: (MonadIO m, MonadHasTime m, ScottyError e) => Process -> ScottyT e m ()
app process = do
  post "/streams/:streamId" $ postEventHandler process
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
