{-# LANGUAGE OverloadedStrings #-}

module GlobalFeedWriter (main) where

import qualified Data.Text             as T
import           EventStoreCommands

main :: DynamoCmdM ()
main = do
  scanResult <- scanNeedsPaging'
  log' Debug $ (T.pack . show . length) scanResult
  main
