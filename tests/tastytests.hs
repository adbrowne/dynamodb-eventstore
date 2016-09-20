{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import           BasicPrelude
import qualified DynamoCmdAmazonkaTests
import qualified DynamoDbEventStore.AmazonkaImplementation  as Ai
import qualified DynamoDbEventStore.DynamoCmdInterpreter as TestInterpreter
import           DynamoDbEventStore.FeedOutputSpec       as FeedOutputSpec
import           DynamoDbEventStore.GlobalFeedWriterSpec as GlobalFeedWriterSpec

import           System.Metrics hiding (Value)
import           Test.Tasty
import           Test.Tasty.Hspec
import qualified WebserverInternalSpec
import           WebserverSpec
import           DodgerBlue.Testing


testInterpreter :: TestInterpreter.DynamoCmdM Queue a -> IO (Either Ai.InterpreterError a)
testInterpreter program =
  return $ Right $ TestInterpreter.evalProgram "Test Program" program TestInterpreter.emptyTestState

nullMetrics :: IO Ai.MetricLogs
nullMetrics = do
   store <- newStore
   return Ai.MetricLogs {
    Ai.metricLogsReadItem = doNothingPair,
    Ai.metricLogsWriteItem = doNothingPair,
    Ai.metricLogsUpdateItem = doNothingPair,
    Ai.metricLogsQuery = doNothingPair,
    Ai.metricLogsScan = doNothingPair,
    Ai.metricLogsStore = store }
  where
    doNothingPair = Ai.MetricLogsPair {
        Ai.metricLogsPairCount = return (),
        Ai.metricLogsPairTimeMs = const $ return () }

main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  getStreamSpec' <- testSpec "Get Stream tests" getStreamSpec
  getEventSpec' <- testSpec "Get Event tests" getEventSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  nullMetricsForAi <- nullMetrics
  defaultMain $
    testGroup "Tests"
      [ testGroup "DynamoCmd Tests against Dynamo - Amazonka" (DynamoCmdAmazonkaTests.tests (Ai.evalProgram nullMetricsForAi)),
        testGroup "DynamoCmd Tests against Test Interpreter" (DynamoCmdAmazonkaTests.tests testInterpreter),
        testGroup "Global Feed Writer" GlobalFeedWriterSpec.tests,
        testGroup "Feed Output" FeedOutputSpec.tests,
        postEventSpec',
        getStreamSpec',
        getEventSpec',
        webserverInternalTests'
      ]
