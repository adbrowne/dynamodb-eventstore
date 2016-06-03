{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module DynamoDbEventStore.Feed (jsonFeed,jsonEntry,Feed(..),recordedEventToFeedEntry,streamResultsToFeed) where

import           BasicPrelude
import           Data.Aeson
import qualified Data.Attoparsec.ByteString            as APBS
import qualified Data.Text                             as T
import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.Vector                           as V
import           DynamoDbEventStore.EventStoreCommands
import           DynamoDbEventStore.EventStoreActions

data Feed
 = Feed
      { feedId       :: Text
      , feedTitle    :: Text
      , feedUpdated  :: UTCTime
      , feedSelfUrl  :: Text
      , feedStreamId :: Text
      , feedAuthor   :: Author
      , feedLinks    :: [Link]
      , feedEntries  :: [Entry]
      }
     deriving (Show)

data Author
 = Author
     { authorName :: Text
     }
     deriving (Show)

data Entry
 = Entry
      { entryId      :: Text
      , entryTitle   :: Text
      , entryUpdated :: UTCTime
      , entryContent :: EntryContent
      , entryLinks   :: [Link]
      , entrySummary :: Text
      }
     deriving (Show)

data EntryContent
 = EntryContent
     { entryContentEventStreamId :: Text
     , entryContentEventNumber   :: Int64
     , entryContentEventType     :: Text
     , entryContentData          :: Maybe Value
     }
     deriving (Show)

data Link
 = Link
      { linkHref :: Text
      , linkRel  :: Text
      }
     deriving (Show)

buildStreamLink :: Text -> Text -> StreamOffset -> Link
buildStreamLink streamUri rel (direction, position, maxItems)=
  let
    positionName = case position of EventStartHead -> "head"
                                    EventStartPosition x -> show x
    directionName = case direction of FeedDirectionForward -> "forward"
                                      FeedDirectionBackward -> "backward"
    href = streamUri <> "/" <> positionName <> "/" <> directionName <> "/" <> show maxItems

  in Link { linkHref = href, linkRel = rel }

streamResultsToFeed :: Text -> StreamId -> UTCTime -> StreamResult -> Feed
streamResultsToFeed baseUri (StreamId streamId) updated StreamResult{..} =
  let
    selfuri = baseUri <> "/streams/" <> streamId
    buildStreamLink' = buildStreamLink selfuri
  in Feed
       { feedId = selfuri
       , feedTitle = "EventStream '" <> streamId <> "'"
       , feedUpdated = updated
       , feedSelfUrl = selfuri
       , feedStreamId = streamId
       , feedAuthor = Author { authorName = "EventStore" }
       , feedLinks = catMaybes [
              buildStreamLink' "first" <$> streamResultFirst
            , buildStreamLink' "last" <$> streamResultLast
            , buildStreamLink' "previous" <$> streamResultPrevious
            , buildStreamLink' "next" <$> streamResultNext
            , Just Link { linkHref = selfuri, linkRel = "self" }
         ]
       , feedEntries = recordedEventToFeedEntry baseUri <$> streamResultEvents
       }

recordedEventToFeedEntry :: Text -> RecordedEvent -> Entry
recordedEventToFeedEntry baseUri recordedEvent =
  let
    streamId = recordedEventStreamId recordedEvent
    eventNumber = (show . recordedEventNumber) recordedEvent
    eventCreated = recordedEventCreated recordedEvent
    eventUri = baseUri <> "/" <> streamId <> "/" <> eventNumber
    title = eventNumber <>  "@" <> streamId
    updated = eventCreated
    summary = recordedEventType recordedEvent
    dataField :: Maybe Value =
      if recordedEventIsJson recordedEvent then
        let
          binaryData = recordedEventData recordedEvent
        in APBS.maybeResult (APBS.parse json binaryData)
      else Nothing
    content = EntryContent {
                  entryContentEventStreamId =  recordedEventStreamId recordedEvent
                , entryContentEventNumber = recordedEventNumber recordedEvent
                , entryContentEventType = recordedEventType recordedEvent
                , entryContentData = dataField
              }
    links = [
        Link { linkHref = eventUri, linkRel = "edit" }
      , Link { linkHref = eventUri, linkRel = "alternatvie" }
      ]
  in Entry {
      entryId = eventUri
      , entryTitle = title
      , entryUpdated = updated
      , entrySummary = summary
      , entryContent = content
      , entryLinks = links
     }

jsonLink :: Link -> Value
jsonLink Link {..} =
  object [ "relation" .= linkRel, "uri" .= linkHref ]

jsonEntryContent :: EntryContent -> Value
jsonEntryContent EntryContent{..} =
  let
    addDataField Nothing xs = xs
    addDataField (Just x) xs = ("data" .= x):xs
    standardFields = [
        "eventStreamId" .= entryContentEventStreamId
      , "eventNumber" .= entryContentEventNumber
      , "eventType" .= entryContentEventType]
  in object $ addDataField entryContentData standardFields

jsonAuthor :: Author -> Value
jsonAuthor Author {..} =
  object [ "name" .= authorName ]

jsonFeed :: Feed -> Value
jsonFeed Feed {..} =
  let
    title = "title" .= feedTitle
    feedid = "id" .= feedId
    headofstream = "headOfStream" .= True
    updated = "updated" .= formatJsonTime feedUpdated
    selfurl = "selfUrl" .= feedSelfUrl
    streamid = "streamId" .= feedStreamId
    etag = "etag" .= ( "todo" :: Text)
    author = "author" .= jsonAuthor feedAuthor
    links = "links" .= (Array . V.fromList) (jsonLink <$> feedLinks)
    entries = "entries" .= (Array . V.fromList) (jsonEntry <$> feedEntries)
  in object [ title, feedid, updated, streamid, author, headofstream, selfurl, etag, links, entries]

formatJsonTime :: UTCTime -> Text
formatJsonTime = T.pack . formatTime defaultTimeLocale (iso8601DateFormat (Just "%H:%M:%SZ"))

jsonEntry :: Entry -> Value
jsonEntry Entry{..} =
  let
    entryid = "id" .= entryId
    title = "title" .= entryTitle
    updated = "updated" .= formatJsonTime entryUpdated
    summary = "summary" .= entrySummary
    content = "content" .= jsonEntryContent entryContent
    links = "links" .= (Array . V.fromList) (jsonLink <$> entryLinks)
  in object [entryid, title, summary, content, links, updated]
