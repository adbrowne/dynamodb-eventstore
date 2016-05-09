{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           BasicPrelude
import qualified DynamoCmdAmazonkaTests
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.GlobalFeedWriterSpec as GlobalFeedWriterSpec
import qualified DynamoDbEventStore.AmazonkaInterpreter as Ai
import qualified DynamoDbEventStore.DynamoCmdInterpreter as TestInterpreter

import           Test.Tasty
import           Test.Tasty.Hspec
import qualified WebserverInternalSpec
import           WebserverSpec


testInterpreter :: DynamoCmdM a -> IO (Either Ai.InterpreterError a)
testInterpreter program =
  return $ Right $ TestInterpreter.evalProgram "Test Program" program TestInterpreter.emptyTestState

main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  getStreamSpec' <- testSpec "Get Stream tests" getStreamSpec
  getEventSpec' <- testSpec "Get Event tests" getEventSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "DynamoCmd Tests against Dynamo - Amazonka" (DynamoCmdAmazonkaTests.tests Ai.evalProgram),
        testGroup "DynamoCmd Tests against Test Interpreter" (DynamoCmdAmazonkaTests.tests testInterpreter),
        testGroup "Global Feed Writer" GlobalFeedWriterSpec.tests,
        postEventSpec',
        getStreamSpec',
        getEventSpec',
        webserverInternalTests'
      ]
