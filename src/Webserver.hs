{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Webserver where

import Control.Applicative
import Web.Scotty

import Data.Monoid (mconcat)

import Data.Char (isDigit)
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import Data.Text.Lazy (Text, pack)
import Data.ByteString (ByteString)
import Network.HTTP.Types (mkStatus)
import Data.Attoparsec.Text.Lazy
import Control.Arrow (left)
import Data.Int

data ExpectedVersion = ExpectedVersion Int
  deriving (Show)

addEvent :: Text -> Int64 -> a -> ActionM ()
addEvent streamId expectedVersion _ = do
    html $ mconcat ["StreamId:", streamId, " expectedVersion:", (pack . show) expectedVersion]

returnEither :: Either a a -> a
returnEither (Left a) = a
returnEither (Right a) = a

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

maxInt64 :: Integer
maxInt64 = toInteger (maxBound :: Int64)

positiveIntegerParser :: Parser Integer
positiveIntegerParser =
  fmap read $ many1 (satisfy isDigit) <* endOfInput

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

app :: ScottyM ()
app = do
  post "/streams/:streamId" $ do
    streamId <- param "streamId"
    expectedVersion <- parseMandatoryHeader "ES-ExpectedVersion" positiveInt64Parser
    eventData <- body
    toResult $
          addEvent
          <$> pure streamId
          <*> expectedVersion
          <*> pure eventData
