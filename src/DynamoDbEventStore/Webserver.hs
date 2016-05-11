{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}

module DynamoDbEventStore.Webserver(app, showEventResponse, positiveInt64Parser, runParser) where

import           BasicPrelude
import           Web.Scotty.Trans

import           Control.Arrow             (left)
import           Data.Attoparsec.Text.Lazy
import           Data.Char                 (isDigit)
import           Control.Monad.Reader
import qualified Data.Text.Lazy            as TL
import qualified Data.Text.Lazy.Encoding   as TL
import qualified Data.Vector               as V
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Time.Clock (UTCTime)
import           Data.Time.Format
import qualified Data.Time.Clock as Time
import           Data.Aeson
import           Network.HTTP.Types.Status
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.AmazonkaInterpreter
import           DynamoDbEventStore.EventStoreCommands

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

positiveInt64Parser :: Parser Int64
positiveInt64Parser =
  filterInt64 =<< positiveIntegerParser
  where
    filterInt64 :: Integer -> Parser Int64
    filterInt64 a
     | a <= maxInt64 = return (fromInteger a)
     | otherwise = fail "too large"

readEventResultJsonValue :: Text -> RecordedEvent -> Value
readEventResultJsonValue baseUri recordedEvent = 
  let
    streamId = recordedEventStreamId recordedEvent
    eventNumber = (show . recordedEventNumber) recordedEvent 
    eventCreated = recordedEventCreated recordedEvent
    eventUri = baseUri <> "/" <> streamId <> "/" <> eventNumber
    title = "title" .= (eventNumber <>  "@" <> streamId)
    updated = "updated" .= (formatTime defaultTimeLocale (iso8601DateFormat (Just "%H:%M:%SZ")) eventCreated)
    summary = "summary" .= recordedEventType recordedEvent
    content = "content" .= object [
        "eventStreamId" .= recordedEventStreamId recordedEvent 
      , "eventNumber" .= recordedEventNumber recordedEvent
      , "eventType" .= recordedEventType recordedEvent
      ]
    links = "links" .= (Array . V.fromList) [
            object [ "relation" .= ("edit" :: Text), "uri" .= eventUri]
         ,  object [ "relation" .= ("alternative" :: Text), "uri" .= eventUri]
      ]
  in object [title, summary, content, links, updated]

eventStoreActionResultToText :: (MonadIO m, ScottyError e) => ResponseEncoding -> EventStoreActionResult -> ActionT e m ()
eventStoreActionResultToText _ (TextResult r) = (raw . TL.encodeUtf8 . TL.fromStrict) r
eventStoreActionResultToText AtomJsonEncoding (PostEventResult r) = (raw . TL.encodeUtf8 . TL.fromStrict) $ show r
eventStoreActionResultToText AtomJsonEncoding (ReadEventResult (Right (Just r))) = (raw . encode . (readEventResultJsonValue "http://localhost:2114")) r
eventStoreActionResultToText AtomJsonEncoding (ReadEventResult (Right Nothing)) = (status $ mkStatus 404 (toByteString "Not Found")) >> raw "{}"
eventStoreActionResultToText AtomJsonEncoding _ = error "todo EventStoreActionResult"

data ResponseEncoding = AtomJsonEncoding

toResult :: (MonadIO m, ScottyError e) => Either LText (IO (Either InterpreterError EventStoreActionResult)) -> ActionT e m ()
toResult (Left err) = error400 err
toResult (Right esResult) = do
  result <- liftIO esResult
  case result of (Left err) -> (error500 . TL.fromStrict . show) err
                 (Right a) -> (eventStoreActionResultToText AtomJsonEncoding) a

showEventResponse :: Show a => a -> IO (Either InterpreterError EventStoreActionResult)
showEventResponse a = return . Right $ TextResult (show a)

notEmpty :: Text -> Either LText Text
notEmpty "" = Left "streamId required"
notEmpty t = Right t

class MonadHasTime m where
  getCurrentTime :: m UTCTime

instance MonadHasTime IO where
  getCurrentTime = Time.getCurrentTime

instance (Monad m) => MonadHasTime (ReaderT UTCTime m) where
  getCurrentTime = ask

app :: (MonadIO m, MonadHasTime m, ScottyError e) => (EventStoreAction -> IO (Either InterpreterError EventStoreActionResult)) -> ScottyT e m ()
app process = do
  post "/streams/:streamId" $ do
    streamId <- param "streamId"
    expectedVersion <- parseOptionalHeader "ES-ExpectedVersion" positiveInt64Parser
    eventType <- parseMandatoryHeader "ES-EventType" textParser
    eventData <- body
    eventTime <- lift getCurrentTime
    let eventEntries = 
          EventEntry
          <$> pure eventData
          <*> (EventType <$> eventType)
    toResult . fmap (process . PostEvent) $
          PostEventRequest
          <$> pure streamId
          <*> expectedVersion
          <*> pure eventTime
          <*> pure False
          <*> ((\x -> x:|[]) <$> eventEntries)
  get "/streams/:streamId/:eventNumber" $ do
    streamId <- param "streamId"
    eventNumber <- param "eventNumber"
    toResult . fmap (process . ReadEvent) $
          ReadEventRequest
          <$> notEmpty streamId
          <*> runParser positiveInt64Parser "Invalid Event Number" eventNumber
  get "/streams/:streamId" $ do
    streamId <- param "streamId"
    let mStreamId = notEmpty streamId
    toResult . fmap (process . ReadStream) $
          ReadStreamRequest
          <$> mStreamId
          <*> Right Nothing
  get "/all" $ 
    toResult . fmap (process . ReadAll) $
          pure ReadAllRequest
  notFound $ status status404
