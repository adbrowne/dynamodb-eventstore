{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import           BasicPrelude
import qualified Prelude as P
import qualified DynamoCmdAmazonkaTests
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.GlobalFeedWriterSpec as GlobalFeedWriterSpec
import qualified DynamoDbEventStore.AmazonkaInterpreter as Ai
import qualified DynamoDbEventStore.DynamoCmdInterpreter as TestInterpreter

import           Test.Tasty
import           Test.Tasty.Hspec
import qualified WebserverInternalSpec
import           WebserverSpec


testInterpreter :: DynamoCmdM a -> IO a
testInterpreter program = do 
  let result = TestInterpreter.evalProgram "Test Program" program TestInterpreter.emptyTestState
  case result of (Left b)  -> error $ P.show b
                 (Right a) -> return a

main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  getStreamSpec' <- testSpec "Get Stream tests" getStreamSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "DynamoCmd Tests against Dynamo - Amazonka" (DynamoCmdAmazonkaTests.tests Ai.evalProgram),
        testGroup "DynamoCmd Tests against Test Interpreter" (DynamoCmdAmazonkaTests.tests testInterpreter),
        testGroup "Global Feed Writer" GlobalFeedWriterSpec.tests,
        postEventSpec',
        getStreamSpec',
        webserverInternalTests'
      ]
