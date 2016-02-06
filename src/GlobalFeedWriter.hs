{-# LANGUAGE OverloadedStrings #-}

module GlobalFeedWriter (main) where

import EventStoreCommands

main :: DynamoCmdM ()
main = log' Debug "Test"
