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

eventCreatedKey :: Text
eventCreatedKey = "EventCreated"

pageBodyKey :: Text
pageBodyKey = "Body"

eventPageNumberKey :: Text
eventPageNumberKey = "PageNumber"
