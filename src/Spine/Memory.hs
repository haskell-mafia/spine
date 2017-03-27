{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}
module Spine.Memory (
  -- * Generic table operations
    TableState (..)
  , Attribute (..)
  , Conditional (..)
  , cast
  , lookupValue
  , newTableState
  , handle
  , put
  , putWith
  , delete
  , deleteFromSet
  , set
  , setWith
  , runSets
  , updateWith
  , alter_
  , alter
  , get
  , getAttribute
  ) where

import           Control.Monad.IO.Class (MonadIO, liftIO)

import qualified Data.List as L
import           Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as H

import           GHC.Show (appPrec, appPrec1, showSpace)

import           P

import           Spine.Data (ItemKey, Key(..), renderKey, toEncoding)

-- |
-- TableState represents a single dynamodb table where the key for the
-- first hashmap is the partition item key and the nested hashmap
-- represents the attributes
--
data TableState a =
  TableState {
      tableState :: IORef (Map a [Attribute])
    }

data Attribute where
  Attribute :: forall b. (Show b, Eq b) => (Key b) -> b -> Attribute

instance Eq Attribute where
  (Attribute k v) == (Attribute k' v') =
    (toEncoding k v == toEncoding k' v')

instance Show Attribute where
  showsPrec p (Attribute k v) =
    showParen (p > appPrec) $
      showString "Attribute " .
        showsPrec appPrec1 k .
        showSpace .
        showsPrec appPrec1 v

cast :: Attribute -> Key b -> Maybe b
cast (Attribute k v) key =
  case (k, key) of
    (IntKey _, IntKey _) ->
      Just v
    (IntSetKey _, IntSetKey _) ->
      Just v
    (StringKey _, StringKey _) ->
      Just v
    (StringSetKey _, StringSetKey _) ->
      Just v
    (BinaryKey _, BinaryKey _) ->
      Just v
    (BinarySetKey _, BinarySetKey _) ->
      Just v
    (TimeKey _, TimeKey _) ->
      Just v
    (BoolKey _, BoolKey _) ->
      Just v
    (NullKey _, NullKey _) ->
      Just v
    (MapKey _, MapKey _) ->
      Just v
    _ ->
      Nothing

lookupValue :: [Attribute] -> Key b -> Maybe b
lookupValue as key =
  L.find (\(Attribute k' _) -> renderKey k' == renderKey key) as >>=
    flip cast key

data Conditional =
    forall a. Exists (Key a)
  | forall a. NotExists (Key a)
  | forall a. Eq a => ExistsValue (Key a) a
  | forall a. Eq a => Contains (Key [a]) [a]
  | forall a. Eq a => NotContains (Key [a]) [a]

newTableState :: MonadIO m => ItemKey a -> m (TableState a)
newTableState _phantom =
  fmap TableState . liftIO $ newIORef H.empty

handle :: [Attribute] -> Conditional -> Bool
handle as cond =
  case cond of
    Exists k -> -- True = exists, False = not exists
      (renderKey k) `L.elem` (fmap (\(Attribute k' _) -> renderKey k') as)
    NotExists a -> -- True = not exists, False = exists
      not . handle as $ Exists a
    ExistsValue k v -> maybe False id $ do -- True = eq, False = not eq
      f <- find (\(Attribute k' _) -> renderKey k' == renderKey k) as
      v' <- cast f k
      pure $ v == v'
    Contains k vs -> -- True = contains, False = not contains
      maybe False (any ((==) True)) $ with (lookupValue as k) $ \xs ->
        fmap (flip L.elem vs) xs
    NotContains k vs ->
      not . handle as $ Contains k vs


put :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> Attribute -> m ()
put state key attr =
  liftIO . atomicModifyIORef' (tableState state) $ \s ->
    (H.insert key [attr] s, ())

putWith :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> Attribute -> Conditional -> m Bool
putWith t p a cond =
  alter t p $ \m ->
    case m of
      Nothing ->
        case handle [] cond of
          False ->
            (Just [], False)
          True ->
            (Just [a], True)
      Just xs ->
        case handle xs cond of
          False ->
            (Just xs, False)
          True ->
            (Just $ a : xs, True)


delete :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> m ()
delete state key =
  liftIO . atomicModifyIORef' (tableState state) $ \s ->
    (H.delete key s, ())

deleteFromSet :: (MonadIO m, Ord a, Eq a, Eq b, Show b) => TableState a -> a -> Key [b] -> b -> m ()
deleteFromSet state key k v =
  alter_ state key $ \m ->
    with m $ \l ->
      with l $ \el@(Attribute k' _) ->
        case renderKey k' == renderKey k of
          False ->
            el
          True ->
            case cast el k of
              Nothing ->
                el
              Just bs ->
                (Attribute k $ L.filter (\b -> not $ b == v) bs)

set :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> [Attribute] -> m ()
set t p as =
  alter_ t p $ \m ->
    case m of
      Nothing ->
        Just as
      Just l ->
        Just $ runSets as l

setWith :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> [Attribute] -> Conditional -> m Bool
setWith t p as cond =
  alter t p $ \m ->
    case m of
      Nothing ->
        case handle [] cond of
          False ->
            (Just [], False)
          True ->
            (Just as, True)
      Just xs ->
        case handle xs cond of
          False ->
            (Just xs, False)
          True ->
            (Just $ runSets as xs, True)

runSets :: [Attribute] -> [Attribute] -> [Attribute]
runSets p ls =
  case p of
    (x : xs) ->
      runSets xs $ runSet x ls
    [] ->
      ls

runSet :: Attribute -> [Attribute] -> [Attribute]
runSet a@(Attribute k _) l =
  case L.partition (\(Attribute k' _) -> renderKey k' == renderKey k) l of
    (_, l') ->
      a : l'

runUpdates :: [Attribute] -> [Attribute] -> [Attribute]
runUpdates p ls =
  case p of
    (x : xs) ->
      runUpdates xs $ runUpdate x ls
    [] ->
      ls

runUpdate :: Attribute -> [Attribute] -> [Attribute]
runUpdate a@(Attribute k v) l =
  case L.partition (\(Attribute k' _) -> renderKey k' == renderKey k) l of
    (xs, l') ->
      join . with xs $ \(Attribute x xv) -> -- should fail if not a singleton?
        case (x, k) of
          (IntSetKey _, IntSetKey _) ->
            (Attribute x (xv <> v)) : l'
          (StringSetKey _, StringSetKey _) ->
            (Attribute x (xv <> v)) : l'
          (BinarySetKey _, BinarySetKey _) ->
            (Attribute x (xv <> v)) : l'
          _ ->
            a : l'

updateWith :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> [Attribute] -> Conditional -> m Bool
updateWith t p as cond =
  alter t p $ \m ->
    case m of
      Nothing ->
        case handle [] cond of
          False ->
            (Nothing, False)
          True ->
            (Just as, True)
      Just xs ->
        case handle xs cond of
          False ->
            (Just xs, False)
          True ->
            (Just $ runUpdates as xs, True)

alter_ :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> (Maybe [Attribute] -> Maybe [Attribute]) -> m ()
alter_ state key op =
  liftIO . atomicModifyIORef' (tableState state) $ \s ->
    (H.alter op key s, ())

alter :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> (Maybe [Attribute] -> (Maybe [Attribute], b)) -> m b
alter state key op =
  liftIO . atomicModifyIORef' (tableState state) $ \s ->
    case op (H.lookup key s) of
      (Nothing, b) ->
        (H.delete key s, b)
      (Just v, b) ->
        (H.insert key v s, b)

get :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> m (Maybe [Attribute])
get state key = do
  h <- liftIO $ readIORef (tableState state)
  pure $ H.lookup key h

getAttribute :: (MonadIO m, Ord a, Eq a) => TableState a -> a -> Key b -> m (Maybe b)
getAttribute state key k = do
  with (get state key) $ \m ->
    m >>=
      flip lookupValue k
