{-# LANGUAGE OverloadedStrings #-}
module DynamoDbEventStore.Constants where

import BasicPrelude

needsPagingKey :: Text
needsPagingKey = "NeedsPaging"

eventCountKey :: Text
eventCountKey = "EventCount"

pageIsVerifiedKey :: Text
pageIsVerifiedKey = "Verified"

pageDynamoKeyPrefix :: Text
pageDynamoKeyPrefix = "page$"

streamDynamoKeyPrefix :: Text
streamDynamoKeyPrefix = "stream$"

pageBodyKey :: Text
pageBodyKey = "Body"

eventPageNumberKey :: Text
eventPageNumberKey = "PageNumber"
