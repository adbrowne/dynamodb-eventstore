{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Webserver where

import Control.Applicative
import Web.Scotty
import Data.Maybe (fromMaybe)
import Data.Monoid (mconcat)
import Safe (readMay)
import Data.Char (isDigit)
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import Data.Text.Lazy (unpack, Text, pack)
import Data.ByteString (ByteString)
import Network.HTTP.Types (Status, mkStatus)
import Data.Either.Utils (maybeToEither, fromEither)
import Data.Attoparsec.Text.Lazy
import Control.Arrow (left)

data ExpectedVersion = ExpectedVersion Int
  deriving (Show)

addEvent :: Text -> Int -> a -> ActionM ()
addEvent streamId expectedVersion eventData = do
    html $ mconcat ["StreamId:", streamId, " expectedVersion:", (pack . show) expectedVersion]

returnEither :: Either a a -> a
returnEither (Left a) = a
returnEither (Right a) = a

toByteString :: Text -> ByteString
toByteString = T.encodeUtf8 . TL.toStrict 

error400 :: Text -> ActionM ()
error400 err = status $ mkStatus 400 (toByteString err)

runParser :: Parser a -> Text -> Text -> Either Text a
runParser p e = (left (const e)) . eitherResult . (parse p)

headerError :: Text -> Text -> Text
headerError headerName message =
  mconcat [headerName, " header ", message]
  
parseMandatoryHeader :: Text -> Parser a -> ActionM (Either Text a)
parseMandatoryHeader headerName parser = do
  headerText <- header headerName
  return $
    maybeToEither missingErrorText headerText
    >>= runParser parser parseFailErrorText
  where 
    missingErrorText = headerError headerName "is missing"
    parseFailErrorText = headerError headerName "in wrong format"

positiveIntegerParser :: Parser Int
positiveIntegerParser =
  fmap read $ many1 (satisfy isDigit) <* endOfInput
  
toResult :: Either Text (ActionM ()) -> ActionM ()
toResult = fromEither . (left error400) 

app = do
  post "/streams/:streamId" $ do
    streamId <- param "streamId"
    expectedVersion <- parseMandatoryHeader "ES-ExpectedVersion" positiveIntegerParser
    eventData <- body
    toResult $ 
          addEvent
          <$> pure streamId
          <*> expectedVersion
          <*> pure eventData
