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
import qualified Data.Text                             as T
import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.UUID
import qualified Data.Vector                           as V
import           DynamoDbEventStore.EventStoreActions
import           DynamoDbEventStore.EventStoreCommands
import           Network.HTTP.Base
import           Text.Blaze
import           Text.Blaze.Internal                   (customLeaf,
                                                        customParent)

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
     , entryContentEventId       :: EventId
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

genericAuthor :: Author
genericAuthor = Author { authorName = "EventStore" }

buildFeed :: Text -> Text -> StreamId -> Text -> UTCTime -> [RecordedEvent] -> [Link] -> Feed
buildFeed baseUri title (StreamId streamId) selfuri updated events links =
  Feed {
    feedId = selfuri
    , feedTitle = title
    , feedUpdated = updated
    , feedSelfUrl = selfuri
    , feedStreamId = streamId
    , feedAuthor = genericAuthor
    , feedLinks = links
    , feedEntries = recordedEventToFeedEntry baseUri <$> events
   }

-- adapted from: http://hackage.haskell.org/package/blaze-html-0.3.2.1/docs/src/Text-Blaze-Renderer-Text.html
escapeHtmlEntities :: Text     -- ^ Text to escape
                   -> Text     -- ^ Resulting text builder
escapeHtmlEntities = T.foldr escape mempty
  where
    escape :: Char -> Text -> Text
    escape '<'  b = "&lt;"   `mappend` b
    escape '>'  b = "&gt;"   `mappend` b
    escape '&'  b = "&amp;"  `mappend` b
    escape '"'  b = "&quot;" `mappend` b
    escape '\'' b = "&#39;"  `mappend` b
    escape x    b = T.singleton x       `mappend` b

streamResultsToFeed :: Text -> StreamId -> UTCTime -> StreamResult -> Feed
streamResultsToFeed baseUri (StreamId streamId) updated StreamResult{..} =
  let
    selfuri = baseUri <> "/streams/" <> urlEncode' streamId
    buildStreamLink' = buildStreamLink selfuri
    links = catMaybes [
              Just Link { linkHref = selfuri, linkRel = "self" }
            , buildStreamLink' "first" <$> streamResultFirst
            , buildStreamLink' "last" <$> streamResultLast
            , buildStreamLink' "next" <$> streamResultNext
            , buildStreamLink' "previous" <$> streamResultPrevious
         ]
    title = "Event stream '" <> escapeHtmlEntities streamId <> "'"
  in buildFeed baseUri title (StreamId streamId) selfuri updated streamResultEvents links

globalStreamResultsToFeed :: Text -> StreamId -> UTCTime -> GlobalStreamResult -> Feed
globalStreamResultsToFeed baseUri streamId updated GlobalStreamResult{..} =
  let
    selfuri = baseUri <> "/streams/%24all"
    buildStreamLink' = buildGlobalStreamLink selfuri
    links = catMaybes [
              Just Link { linkHref = selfuri, linkRel = "self" }
            , buildStreamLink' "first" <$> globalStreamResultFirst
            , buildStreamLink' "last" <$> globalStreamResultLast
            , buildStreamLink' "next" <$> globalStreamResultNext
            , buildStreamLink' "previous" <$> globalStreamResultPrevious
         ]
  in buildFeed baseUri "All events" streamId selfuri updated globalStreamResultEvents links


recordedEventToFeedEntry :: Text -> RecordedEvent -> Entry
recordedEventToFeedEntry baseUri recordedEvent =
  let
    streamId = recordedEventStreamId recordedEvent
    eventNumber = (show . recordedEventNumber) recordedEvent
    eventCreated = recordedEventCreated recordedEvent
    eventUri = baseUri <> "/streams/" <> urlEncode' streamId <> "/" <> eventNumber
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
                , entryContentEventId = recordedEventId recordedEvent
                , entryContentData = dataField
              }
    links = [
        Link { linkHref = eventUri, linkRel = "edit" }
      , Link { linkHref = eventUri, linkRel = "alternate" }
      ]
  in Entry {
      entryTitle = title
      , entryId = eventUri
      , entryUpdated = updated
      , entrySummary = summary
      , entryContent = content
      , entryLinks = links
     }

urlEncode' :: Text -> Text
urlEncode' = T.pack . urlEncode . T.unpack

jsonLink :: Link -> Value
jsonLink Link {..} =
  object [ "relation" .= linkRel, "uri" .= linkHref ]

xmlLink :: Link -> Markup
xmlLink Link {..} =
  customLeaf "link" True
    ! customAttribute "href" (textValue linkHref)
    ! customAttribute "rel" (textValue linkRel)

xmlEntry :: Entry -> Markup
xmlEntry Entry{..} =
  let
    entryid = simpleXmlNode "id" entryId
    title =  simpleXmlNode "title" entryTitle
    updated = simpleXmlNode "updated" $ formatJsonTime entryUpdated
    summary = simpleXmlNode "summary" entrySummary
    links = xmlLink <$> entryLinks
  in customParent "entry" $ do
      title
      entryid
      updated
      xmlAuthor genericAuthor
      summary
      forM_ links id

jsonEntryContent :: EntryContent -> Value
jsonEntryContent EntryContent{..} =
  let
    addDataField Nothing xs = xs
    addDataField (Just x) xs = ("data" .= x):xs
    standardFields = [
        "eventStreamId" .= entryContentEventStreamId
      , "eventNumber" .= entryContentEventNumber
      , "eventId" .= Data.UUID.toText (unEventId entryContentEventId)
      , "eventType" .= entryContentEventType]
  in object $ addDataField entryContentData standardFields

jsonAuthor :: Author -> Value
jsonAuthor Author {..} =
  object [ "name" .= authorName ]

simpleXmlNode :: Tag -> Text -> Markup
simpleXmlNode tagName tagContent =
  customParent tagName $ text tagContent

xmlAuthor :: Author -> Markup
xmlAuthor Author {..} =
  customParent "author" $ simpleXmlNode "name" authorName

xmlFeed :: Feed -> Markup
xmlFeed Feed {..} =
  let
    feed = customParent "feed" ! customAttribute "xmlns" "http://www.w3.org/2005/Atom"
    title = customParent "title" (preEscapedText feedTitle)
    feedid = simpleXmlNode "id" feedId
    updated = simpleXmlNode "updated" $ formatJsonTime feedUpdated
    author = xmlAuthor feedAuthor
    links = xmlLink <$> feedLinks
    entries = xmlEntry <$> feedEntries
  in feed $ do
    title
    feedid
    updated
    author
    forM_ links id
    forM_ entries id

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
formatJsonTime utcTime =
  let
    microseconds = take 6 $ formatTime defaultTimeLocale "%q" utcTime
    dateAndTime = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S" utcTime
  in T.pack $ dateAndTime <> "." <> microseconds <> "Z"

jsonEntry :: Entry -> Value
jsonEntry Entry{..} =
  let
    entryid = "id" .= entryId
    title = "title" .= entryTitle
    author = "author" .= (jsonAuthor genericAuthor)
    updated = "updated" .= formatJsonTime entryUpdated
    summary = "summary" .= entrySummary
    content = "content" .= jsonEntryContent entryContent
    links = "links" .= (Array . V.fromList) (jsonLink <$> entryLinks)
  in object [entryid, author, title, summary, content, links, updated]
