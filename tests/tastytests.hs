{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import qualified BasicOperationTests     as CommandTests
import           DynamoDbEventStore.Testing
import           DynamoDbEventStore.EventStoreActionTests as ActionTests
import qualified DynamoDbEventStore.AmazonkaInterpreter as Ai

import           Test.Tasty
import           Test.Tasty.Hspec
import qualified WebserverInternalSpec
import           WebserverSpec


main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  getStreamSpec' <- testSpec "Get Stream tests" getStreamSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "Command Unit Tests" (CommandTests.tests evalProgram),
        testGroup "Command Tests against Dynamo - Amazonka" (CommandTests.tests Ai.evalProgram),
        testGroup "Action Tests" ActionTests.tests,
        postEventSpec',
        getStreamSpec',
        webserverInternalTests'
      ]
