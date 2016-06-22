{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module DynamoDbEventStore.Feed (
  xmlFeed,
  jsonFeed,
  jsonEntry,
  Feed(..),
  recordedEventToFeedEntry,
  streamResultsToFeed,
  globalStreamResultsToFeed,
  globalFeedPositionToText) where

import           BasicPrelude
import           Data.Aeson
import qualified Data.Attoparsec.ByteString            as APBS
import           Data.HashMap.Strict                   as HM
import qualified Data.Text                             as T
import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.Vector                           as V
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import           Text.Taggy.DOM

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

globalFeedPositionToText :: GlobalFeedPosition -> Text
globalFeedPositionToText GlobalFeedPosition{..} = show globalFeedPositionPage <> "-" <> show globalFeedPositionOffset

buildGlobalStreamLink :: Text -> Text -> GlobalStreamOffset -> Link
buildGlobalStreamLink streamUri rel (direction, position, maxItems)=
  let
    positionName = case position of GlobalStartHead -> "head"
                                    GlobalStartPosition x -> globalFeedPositionToText x
    directionName = case direction of FeedDirectionForward -> "forward"
                                      FeedDirectionBackward -> "backward"
    href = streamUri <> "/" <> positionName <> "/" <> directionName <> "/" <> show maxItems

  in Link { linkHref = href, linkRel = rel }

buildFeed :: Text -> Text -> StreamId -> Text -> UTCTime -> [RecordedEvent] -> [Link] -> Feed
buildFeed baseUri title (StreamId streamId) selfuri updated events links =
  Feed {
    feedId = selfuri
    , feedTitle = title
    , feedUpdated = updated
    , feedSelfUrl = selfuri
    , feedStreamId = streamId
    , feedAuthor = Author { authorName = "EventStore" }
    , feedLinks = links
    , feedEntries = recordedEventToFeedEntry baseUri <$> events
   }

streamResultsToFeed :: Text -> StreamId -> UTCTime -> StreamResult -> Feed
streamResultsToFeed baseUri (StreamId streamId) updated StreamResult{..} =
  let
    selfuri = baseUri <> "/streams/" <> streamId
    buildStreamLink' = buildStreamLink selfuri
    links = catMaybes [
              buildStreamLink' "first" <$> streamResultFirst
            , buildStreamLink' "last" <$> streamResultLast
            , buildStreamLink' "previous" <$> streamResultPrevious
            , buildStreamLink' "next" <$> streamResultNext
            , Just Link { linkHref = selfuri, linkRel = "self" }
         ]
    title = "EventStream '" <> streamId <> "'"
  in buildFeed baseUri title (StreamId streamId) selfuri updated streamResultEvents links

globalStreamResultsToFeed :: Text -> StreamId -> UTCTime -> GlobalStreamResult -> Feed
globalStreamResultsToFeed baseUri streamId updated GlobalStreamResult{..} =
  let
    selfuri = baseUri <> "/streams/%24all"
    buildStreamLink' = buildGlobalStreamLink selfuri
    links = catMaybes [
              buildStreamLink' "first" <$> globalStreamResultFirst
            , buildStreamLink' "last" <$> globalStreamResultLast
            , buildStreamLink' "previous" <$> globalStreamResultPrevious
            , buildStreamLink' "next" <$> globalStreamResultNext
            , Just Link { linkHref = selfuri, linkRel = "self" }
         ]
  in buildFeed baseUri "All events" streamId selfuri updated globalStreamResultEvents links


recordedEventToFeedEntry :: Text -> RecordedEvent -> Entry
recordedEventToFeedEntry baseUri recordedEvent =
  let
    streamId = recordedEventStreamId recordedEvent
    eventNumber = (show . recordedEventNumber) recordedEvent
    eventCreated = recordedEventCreated recordedEvent
    eventUri = baseUri <> "/streams/" <> streamId <> "/" <> eventNumber
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
      , Link { linkHref = eventUri, linkRel = "alternate" }
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

xmlLink :: Link -> Node
xmlLink Link {..} =
  let
    attrs = [
        ("rel", linkRel)
      , ("href", linkHref)
            ]
  in NodeElement Element { eltName = "link", eltAttrs = HM.fromList attrs, eltChildren = [] }

xmlEntry :: Entry -> Node
xmlEntry Entry{..} =
  let
    entryid = simpleXmlNode "id" entryId
    title =  simpleXmlNode "title" entryTitle
    updated = simpleXmlNode "updated" $ formatJsonTime entryUpdated
    summary = simpleXmlNode "summary" entrySummary
    links = xmlLink <$> entryLinks
    children = [
      entryid
      , title
      , updated
      , summary
               ] ++ links
  in NodeElement Element { eltName = "entry", eltAttrs = mempty, eltChildren = children }

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

simpleXmlNode :: Text -> Text -> Node
simpleXmlNode tagName tagContent =
  NodeElement Element { eltName = tagName, eltAttrs = mempty, eltChildren = [NodeContent tagContent]}

xmlAuthor :: Author -> Node
xmlAuthor Author {..} =
  NodeElement Element { eltName = "author", eltAttrs = mempty, eltChildren = [simpleXmlNode "name" authorName]}

xmlFeed :: Feed -> Node
xmlFeed Feed {..} =
  let
    title = simpleXmlNode "title" feedTitle
    feedid = simpleXmlNode "id" feedId
    headofstream = simpleXmlNode "headOfStream" "True"
    updated = simpleXmlNode "updated" $ formatJsonTime feedUpdated
    selfurl = simpleXmlNode "selfUrl" feedSelfUrl
    streamid = simpleXmlNode "streamId" feedStreamId
    etag = simpleXmlNode "etag" ( "todo" :: Text)
    author = xmlAuthor feedAuthor
    links = xmlLink <$> feedLinks
    entries = xmlEntry <$> feedEntries
    children = [
      title
      , feedid
      , headofstream
      , updated
      , selfurl
      , streamid
      , etag
      , author
     ] ++ links ++ entries
  in NodeElement Element {
    eltName = "feed"
    , eltAttrs = HM.singleton "xmlns" "http://www.w3.org/2005/Atom"
    , eltChildren = children }

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
