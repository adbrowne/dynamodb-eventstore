{-# LANGUAGE RecordWildCards #-}

module DynamoDbEventStore.InMemoryCache
  (newCache
  ,emptyCache
  ,insertCache
  ,lookupCache
  ,Cache(..)
  ,Caches) where

import           BasicPrelude
import qualified Data.Cache.LRU                        as LRU
import           Data.Dynamic
import qualified Data.Map.Strict                       as Map
import           Safe

data Caches = Caches
    { cachesNextIndex :: Int
    , cachesCacheMap  :: Map Int (Integer, Maybe Dynamic)
    } deriving ((Show))

emptyCache :: Caches
emptyCache =
  Caches
  {
    cachesNextIndex = 0
  , cachesCacheMap = mempty
  }

newtype Cache k v = Cache
    { unCache :: Int
    } deriving ((Show))

newCache :: Integer -> Caches -> (Cache k v, Caches)
newCache size Caches{..} =
    let cacheKey =
            Cache
            { unCache = cachesNextIndex
            }
        caches =
            Caches
            { cachesNextIndex = cachesNextIndex + 1
            , cachesCacheMap = Map.insert
                  cachesNextIndex
                  (size, Nothing)
                  cachesCacheMap
            }
    in (cacheKey, caches)

insertCache
    :: (Typeable k, Ord k, Typeable v)
    => Cache k v -> k -> v -> Caches -> Caches
insertCache Cache{..} key value caches@Caches{..} =
    let cache = fromJustNote "insert: could not find cache" $ Map.lookup unCache cachesCacheMap
        updatedCache = insertItem cache
    in caches
       { cachesCacheMap = Map.insert unCache updatedCache cachesCacheMap
       }
  where
    insertItem (size, Nothing) =
      let cache = LRU.insert key value (LRU.newLRU (Just size))
      in (size, Just (toDyn cache))
    insertItem (size,Just dyn) =
        let
          cache =
            fromDyn
                dyn
                (error
                      "InMemoryCaches.insertCache Invalid format for cache")
          cache' = LRU.insert key value cache
        in (size, Just (toDyn cache'))

lookupCache
    :: (Typeable k, Ord k, Typeable v)
    => Cache k v -> k -> Caches -> (Maybe v, Caches)
lookupCache Cache{..} key caches@Caches{..} =
    let (size, cache) = fromJustNote "insert: could not find cache" $ Map.lookup unCache cachesCacheMap
        (cache', result) = go cache
    in ( result,
         caches
         { cachesCacheMap = Map.insert unCache (size, cache') cachesCacheMap })
    where
      go Nothing = (Nothing, Nothing)
      go (Just dyn) =
        let
          cache =
            fromDyn
                dyn
                (error
                      "InMemoryCaches.lookupCache Invalid format for cache")
          (cache', result) = LRU.lookup key cache
        in (Just (toDyn cache'), result)
