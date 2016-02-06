{-# LANGUAGE OverloadedStrings #-}
module DynamoDbEventStore.Constants where

import qualified Data.Text             as T

needsPagingKey :: T.Text
needsPagingKey = "NeedsPaging"
