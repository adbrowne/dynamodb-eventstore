{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import qualified DynamoCmdAmazonkaTests
import           DynamoDbEventStore.GlobalFeedWriterSpec as GlobalFeedWriterSpec
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
      [ testGroup "DynamoCmd Tests against Dynamo - Amazonka" (DynamoCmdAmazonkaTests.tests Ai.evalProgram),
        testGroup "Global Feed Writer" GlobalFeedWriterSpec.tests,
        postEventSpec',
        getStreamSpec',
        webserverInternalTests'
      ]
