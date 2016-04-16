{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module DynamoDbEventStore.Webserver(app, showEventResponse, positiveInt64Parser, runParser) where

import           Web.Scotty

import           Control.Arrow             (left)
import           Data.Attoparsec.Text.Lazy
import           Data.ByteString           (ByteString)
import           Data.Char                 (isDigit)
import           Data.Int
import qualified Data.Text.Encoding        as T
import qualified Data.Text                 as T
import           Data.Text.Lazy            (Text, pack)
import qualified Data.Text.Lazy            as TL
import           Network.HTTP.Types.Status
import           DynamoDbEventStore.EventStoreActions

data ExpectedVersion = ExpectedVersion Int
  deriving (Show)

toByteString :: Text -> ByteString
toByteString = T.encodeUtf8 . TL.toStrict

error400 :: Text -> ActionM ()
error400 err = status $ mkStatus 400 (toByteString err)

runParser :: Parser a -> e -> Text -> Either e a
runParser p e = left (const e) . eitherResult . parse p

headerError :: Text -> Text -> Text
headerError headerName message =
  mconcat [headerName, " header ", message]

maybeToEither :: a -> Maybe b ->Either a b
maybeToEither a Nothing = Left a
maybeToEither _ (Just a) = Right a

parseMandatoryHeader :: Text -> Parser a -> ActionM (Either Text a)
parseMandatoryHeader headerName parser = do
  headerText <- header headerName
  return $
    maybeToEither missingErrorText headerText
    >>= runParser parser parseFailErrorText
  where
    missingErrorText = headerError headerName "is missing"
    parseFailErrorText = headerError headerName "in wrong format"

parseOptionalHeader :: Text -> Parser a -> ActionM (Either Text (Maybe a))
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
  fmap read $ many1 (satisfy isDigit) <* endOfInput

textParser :: Parser T.Text
textParser =
  fmap T.pack $ many1 (satisfy (const True)) <* endOfInput

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

toResult :: Either Text (ActionM ()) -> ActionM ()
toResult = fromEither . left error400

showEventResponse :: Show a => a -> ActionM ()
showEventResponse = html . pack . show

notEmpty :: T.Text -> Either Text T.Text
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
          <*> eventType
    toResult . fmap (process . PostEvent) $
          PostEventRequest
          <$> pure streamId
          <*> expectedVersion
          <*> ((\x -> x:[]) <$> eventEntries)
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
