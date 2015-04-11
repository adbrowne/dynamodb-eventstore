{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           Control.Monad.Free
import           Control.Monad.State
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import           Data.Map                (Map)
import qualified Data.Map                as M
import           Data.Int
import qualified Data.Text               as T
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import           EventStoreActions
import           EventStoreCommands
import qualified BasicOperationTests     as CommandTests
import           DynamoDbEventStore.Testing
import           DynamoDbEventStore.EventStoreActionTests as ActionTests
import qualified DynamoDbEventStore.DynamoInterpreter as Di

import           Test.Tasty
import           Test.Tasty.Hspec
import           Test.Tasty.HUnit
import qualified WebserverInternalSpec
import           WebserverSpec


main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "Command Unit Tests" (CommandTests.tests evalProgram),
        --testGroup "Command Tests against Dynamo" (CommandTests.tests Di.evalProgram),
        testGroup "Action Tests" ActionTests.tests,
        postEventSpec',
        webserverInternalTests'
      ]
