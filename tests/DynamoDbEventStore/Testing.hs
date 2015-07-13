{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RecordWildCards           #-}
module DynamoDbEventStore.Testing where

import           Control.Applicative
import           Control.Monad.Free
import           Control.Monad.State
import           Data.Int
import qualified Data.List               as L
import           Data.Map                (Map)
import           Data.Maybe              (fromMaybe)
import qualified Data.Map                as M
import           Data.Monoid
import qualified Data.ByteString         as BS
import qualified Data.Text               as T
import           EventStoreCommands
import           Test.Tasty.QuickCheck
import           TextShow

type FakeEventTable = Map StreamId (Map Int64 (EventType, BS.ByteString, Maybe PageKey))
type FakePageTable = Map PageKey (PageStatus, [EventKey])
type Log = [T.Text]
type FakeState = (FakeEventTable, FakePageTable, Log)

getEventTable (a,_,_) = a
getPageTable (_,a,_) = a
getLog (_,_,a) = a

emptyTestState :: FakeState
emptyTestState = (M.empty, M.empty, [])

writeEventToState :: (t -> t1) -> (t, t2, l) -> (t1, t2, l)
writeEventToState f (eT, pT, l) =
   (f eT, pT, l)

writePageToState :: (t -> t2) -> (t1, t, l) -> (t1, t2, l)
writePageToState f (eT, pT, l) =
   (eT, f pT, l)

writeEventToTable :: EventKey -> (EventType, BS.ByteString, Maybe PageKey) -> FakeEventTable -> FakeEventTable
writeEventToTable (EventKey (sId, evtNumber)) v t =
  let
    events = fromMaybe M.empty $ M.lookup sId t
    events' = M.insert evtNumber v events
  in
    M.insert sId events' t

updateEvent :: (a -> a) -> EventKey -> Map StreamId (Map Int64 a) -> Map StreamId (Map Int64 a)
updateEvent f (EventKey (sId, evtNumber)) t =
  let
    newTable = do
      events <- M.lookup sId t
      event <- M.lookup evtNumber events
      let event' = f event
      let events' = M.insert evtNumber event' events
      let t' = M.insert sId events' t
      return t'
  in
    fromMaybe t newTable

lookupEventKey :: EventKey -> Map StreamId (Map Int64 b) -> Maybe b
lookupEventKey (EventKey (sId, evtNumber)) t =
  M.lookup sId t >>= M.lookup evtNumber

runCmd :: MonadState FakeState m => EventStoreCmd (m a) -> m a
runCmd (Wait' n) = n ()
runCmd (GetEvent' k f) =
  f . (lookupEventKey k) =<< gets getEventTable
runCmd (GetEventsBackward' k _ _ f) =
  f . (reverse . (getEvents k)) =<< gets getEventTable
    where
      getEvents (StreamId sId) table =
        let
          events = fromMaybe M.empty $ M.lookup (StreamId sId) table
        in do
          (evtNumber, (t,d,_)) <- M.assocs events
          return (RecordedEvent sId evtNumber d t)

runCmd (WriteEvent' k t v n) = do
  exists <- gets (lookupEventKey k . getEventTable)
  result <- writeEvent exists k t v
  n result
    where
      writeEvent Nothing _ _ _ = do
        modify $ writeEventToState $ writeEventToTable k (t,v,Nothing) --M.insert k (t, v, Nothing)
        return WriteSuccess
      writeEvent (Just _) _ _ _ =
        return EventExists
runCmd (SetEventPage' k pk n) = do
  let f (et, eb, _) = (et, eb, Just pk)
  modify $ writeEventToState $ updateEvent f k
  n SetEventPageSuccess
runCmd (ScanUnpagedEvents' n) = do
  allItems <- gets (buildAllItems . getEventTable)
  let unpaged = filter unpagedEntry allItems
  n $ fmap fst unpaged
  where
    buildAllItems :: FakeEventTable -> [(EventKey, Maybe PageKey)]
    buildAllItems t =
      let
         kv = M.assocs t
         flat = do
           (sId, evts) <- kv
           (evtNum, (_,_,pk)) <- M.assocs evts
           return (EventKey (sId, evtNum), pk)
      in
         flat
    unpagedEntry (_, Just _) = False
    unpagedEntry (_, Nothing) = True
runCmd (GetPageEntry' k n) =
  n =<< gets (M.lookup k . getPageTable)
runCmd (WritePageEntry' k PageWriteRequest {..} n) = do
  table <- gets getPageTable
  let entryStatus = fst <$> M.lookup k table
  let writeResult = writePage expectedStatus entryStatus table
  modify $ writePageToState (modifyPage writeResult)
  n $ fmap fst writeResult
    where
      doInsert = M.insert k (newStatus, entries)
      writePage :: Maybe PageStatus -> Maybe PageStatus -> FakePageTable -> Maybe (PageStatus, FakePageTable)
      writePage Nothing Nothing table = Just (newStatus,  doInsert table)
      writePage (Just c) (Just e) table
        | c == e =  Just (newStatus, doInsert table)
        | otherwise = Nothing
      writePage _ _ _ = Nothing
      modifyPage :: Maybe (PageStatus, FakePageTable) -> FakePageTable -> FakePageTable
      modifyPage (Just (_, pT)) _ = pT
      modifyPage Nothing s = s

runTest :: MonadState FakeState m => EventStoreCmdM a -> m a
runTest = iterM runCmd

runCmdGen :: EventStoreCmd (StateT FakeState Gen a) -> StateT FakeState Gen a
runCmdGen (Wait' n) = n ()
runCmdGen (GetEvent' k f) = do
  eventTable <- gets getEventTable
  let event = lookupEventKey k eventTable
  writeLog ("GetEvent " <> showt k <> " " <> showt event)
  f event
runCmdGen (GetEventsBackward' k _ _ f) =
  f . (reverse . (getEvents k)) =<< gets getEventTable
    where
      getEvents (StreamId sId) table =
        let
          events = fromMaybe M.empty $ M.lookup (StreamId sId) table
        in do
          (evtNumber, (t,d,_)) <- M.assocs events
          return (RecordedEvent sId evtNumber d t)

runCmdGen (WriteEvent' k t v n) = do
  exists <- gets (lookupEventKey k . getEventTable)
  result <- writeEvent exists k t v
  n result
    where
      writeEvent Nothing _ _ _ = do
        modify $ writeEventToState $ writeEventToTable k (t,v,Nothing) --M.insert k (t, v, Nothing)
        return WriteSuccess
      writeEvent (Just _) _ _ _ =
        return EventExists
runCmdGen (SetEventPage' k pk n) = do
  let f (et, eb, _) = (et, eb, Just pk)
  modify $ writeEventToState $ updateEvent f k
  writeLog ("SetEventPage: " <> (showt k) <> " " <> (showt pk))
  n SetEventPageSuccess
runCmdGen (ScanUnpagedEvents' n) = do
  allItems <- gets (buildAllItems . getEventTable)
  let unpaged = filter unpagedEntry allItems
  toReturn <- lift (sublistOf (fmap fst unpaged))
  n toReturn
  where
    buildAllItems :: FakeEventTable -> [(EventKey, Maybe PageKey)]
    buildAllItems t =
      let
         kv = M.assocs t
         flat = do
           (sId, evts) <- kv
           (evtNum, (_,_,pk)) <- M.assocs evts
           return (EventKey (sId, evtNum), pk)
      in
         flat
    unpagedEntry (_, Just _) = False
    unpagedEntry (_, Nothing) = True
runCmdGen (GetPageEntry' k n) =
  n =<< gets (M.lookup k . getPageTable)
runCmdGen (WritePageEntry' k PageWriteRequest {..} n) = do
  table <- gets getPageTable
  let entryStatus = fst <$> M.lookup k table
  let writeResult = writePage expectedStatus entryStatus table
  modify $ writePageToState (modifyPage writeResult)
  writeLog "WritePageEntry"
  n $ fmap fst writeResult
    where
      doInsert = M.insert k (newStatus, entries)
      writePage :: Maybe PageStatus -> Maybe PageStatus -> FakePageTable -> Maybe (PageStatus, FakePageTable)
      writePage Nothing Nothing table = Just (newStatus,  doInsert table)
      writePage (Just c) (Just e) table
        | c == e =  Just (newStatus, doInsert table)
        | otherwise = Nothing
      writePage _ _ _ = Nothing
      modifyPage :: Maybe (PageStatus, FakePageTable) -> FakePageTable -> FakePageTable
      modifyPage (Just (_, pT)) _ = pT
      modifyPage Nothing s = s

writeLog :: T.Text -> StateT FakeState Gen ()
writeLog msg = do
  modify $ addToLog
  where
    addToLog (a, b, log) = (a, b, msg:log)

runTestGen :: EventStoreCmdM a -> StateT FakeState Gen a
runTestGen = iterM runCmdGen

evalProgram :: EventStoreCmdM a -> IO a
evalProgram program = return $ evalState (runTest program) (M.empty, M.empty, [])
