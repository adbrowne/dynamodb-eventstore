{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import           BasicPrelude
import           DynamoDbEventStore.FeedOutputSpec       as FeedOutputSpec

import           Test.Tasty
import           Test.Tasty.Hspec
import qualified WebserverInternalSpec
import           WebserverSpec
import qualified DynamoDbEventStore.PagingSpec as PagingSpec
import qualified DynamoDbEventStore.GlobalPagingSpec as GlobalPagingSpec


main :: IO ()
main = do
  postEventSpec' <- testSpec "Post Event tests" postEventSpec
  getStreamSpec' <- testSpec "Get Stream tests" getStreamSpec
  getEventSpec' <- testSpec "Get Event tests" getEventSpec
  webserverInternalTests' <- testSpec "Webserver Internal Tests" WebserverInternalSpec.spec
  defaultMain $
    testGroup "Tests"
      [ testGroup "Feed Output" FeedOutputSpec.tests,
        postEventSpec',
        getStreamSpec',
        getEventSpec',
        webserverInternalTests',
        testGroup "Paging tests" PagingSpec.tests,
        testGroup "Global Paging tests" GlobalPagingSpec.tests
      ]
