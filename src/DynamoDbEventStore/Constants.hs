{-# LANGUAGE OverloadedStrings #-}
module DynamoDbEventStore.Constants where

import qualified Data.Text             as T

needsPagingKey :: T.Text
needsPagingKey = "NeedsPaging"

pageIsVerifiedKey :: T.Text
pageIsVerifiedKey = "Verified"

pageDynamoKeyPrefix :: T.Text
pageDynamoKeyPrefix = "page$"

streamDynamoKeyPrefix :: T.Text
streamDynamoKeyPrefix = "stream$"

pageBodyKey :: T.Text
pageBodyKey = "Body"

eventPageNumberKey :: T.Text
eventPageNumberKey = "PageNumber"
