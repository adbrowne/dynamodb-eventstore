{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
module DynamoDbEventStore.Constants where

import           BasicPrelude

needsPagingKey :: Text
needsPagingKey = "NeedsPaging"

eventCountKey :: Text
eventCountKey = "EventCount"

pageIsVerifiedKey :: Text
pageIsVerifiedKey = "Verified"

streamDynamoKeyPrefix :: Text
streamDynamoKeyPrefix = "stream$"

eventCreatedKey :: Text
eventCreatedKey = "EventCreated"

isJsonKey :: Text
isJsonKey = "IsJson"

eventPageNumberKey :: Text
eventPageNumberKey = "PageNumber"
