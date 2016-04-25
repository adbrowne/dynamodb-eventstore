{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import BasicPrelude
import Network.Wreq
import Control.Lens
import           System.Random
import qualified Data.Text               as T
import           Data.Aeson

main :: IO ()
main = do
  streamIdNum :: Int <- getStdRandom (randomR (1,9999999999))
  let streamId = "foo" ++ ((T.unpack . show) streamIdNum) -- random stream
  let streamUri = "http://localhost:3000/streams/" ++ streamId
  let opts = defaults
                 & header "ES-ExpectedVersion" .~ ["0"]
                 & header "ES-EventType" .~ ["EventType0"]
  _ <- postWith opts streamUri (toJSON ("content" :: T.Text))
  r <- get streamUri
  putStrLn $ show $ r ^. responseBody
