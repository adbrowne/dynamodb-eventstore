{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import qualified BasicOperationTests     as CommandTests
import qualified DynamoCmdAmazonkaTests
import           DynamoDbEventStore.Testing
import           DynamoDbEventStore.GlobalFeedWriterSpec as GlobalFeedWriterSpec
import qualified DynamoDbEventStore.AmazonkaInterpreter as Ai
import qualified DynamoDbEventStore.AmazonkaInterpreterNew as Ain

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
        testGroup "DynamoCmd Tests against Dynamo - Amazonka" (DynamoCmdAmazonkaTests.tests Ain.evalProgram),
        testGroup "Global Feed Writer" GlobalFeedWriterSpec.tests,
        postEventSpec',
        getStreamSpec',
        webserverInternalTests'
      ]
