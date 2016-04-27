{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module DynamoDbEventStore.Webserver(app, showEventResponse, positiveInt64Parser, runParser) where

import           BasicPrelude
import           Web.Scotty

import           Control.Arrow             (left)
import           Data.Attoparsec.Text.Lazy
import           Data.Char                 (isDigit)
import qualified Data.Text.Lazy            as TL
import           Network.HTTP.Types.Status
import           DynamoDbEventStore.EventStoreActions

data ExpectedVersion = ExpectedVersion Int
  deriving (Show)

toByteString :: LText -> ByteString
toByteString = encodeUtf8 . TL.toStrict

error400 :: LText -> ActionM ()
error400 err = status $ mkStatus 400 (toByteString err)

runParser :: Parser a -> e -> LText -> Either e a
runParser p e = left (const e) . eitherResult . parse p

headerError :: LText -> LText -> LText
headerError headerName message =
  mconcat [headerName, " header ", message]

maybeToEither :: a -> Maybe b ->Either a b
maybeToEither a Nothing = Left a
maybeToEither _ (Just a) = Right a

parseMandatoryHeader :: LText -> Parser a -> ActionM (Either LText a)
parseMandatoryHeader headerName parser = do
  headerText <- header headerName
  return $
    maybeToEither missingErrorText headerText
    >>= runParser parser parseFailErrorText
  where
    missingErrorText = headerError headerName "is missing"
    parseFailErrorText = headerError headerName "in wrong format"

parseOptionalHeader :: LText -> Parser a -> ActionM (Either LText (Maybe a))
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

fromEither :: Either a a -> a
fromEither (Left a) = a
fromEither (Right a) = a

toResult :: Either LText (ActionM ()) -> ActionM ()
toResult = fromEither . left error400

showEventResponse :: Show a => a -> ActionM ()
showEventResponse = html . TL.fromStrict . show

notEmpty :: Text -> Either LText Text
notEmpty "" = Left "streamId required"
notEmpty t = Right t

app :: (EventStoreAction -> ActionM ()) -> ScottyM ()
app process = do
  post "/streams/:streamId" $ do
    streamId <- param "streamId"
    expectedVersion <- parseOptionalHeader "ES-ExpectedVersion" positiveInt64Parser
    eventType <- parseMandatoryHeader "ES-EventType" textParser
    eventData <- body
    let eventEntries = 
          EventEntry
          <$> pure eventData
          <*> (EventType <$> eventType)
    toResult . fmap (process . PostEvent) $
          PostEventRequest
          <$> pure streamId
          <*> expectedVersion
          <*> ((\x -> x:[]) <$> eventEntries)
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
