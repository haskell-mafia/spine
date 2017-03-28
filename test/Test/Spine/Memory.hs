{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Spine.Memory where

import           Disorder.Core.IO
import           Disorder.Corpus

import           P

import           Spine.Data
import           Spine.Memory

import           Test.Spine.Arbitrary ()
import           Test.QuickCheck

prop_cast a@(Attribute key _) =
  isJust (cast a key) === True

g :: ItemKey Text
g =
  ItemStringKey "item.test"

k :: Key Text
k =
  StringKey "test"

ki :: Key Int
ki =
  IntKey "test"

ks :: Key [Text]
ks =
  StringSetKey "tests"

prop_handle = forAll (elements simpsons) $ \v ->
  conjoin [
      handle [Attribute k v] (Exists k) === True
    , handle [Attribute k v] (NotExists k) === False
    ]

prop_put_get = forAll (elements simpsons) $ \v ->
  testIO $ do
    state <- newTableState g
    put state v (Attribute k v)
    r <- get state v
    pure $ r === Just [Attribute k v]

prop_put_get_attribute = forAll (elements simpsons) $ \v ->
  testIO $ do
    state <- newTableState g
    put state v (Attribute k v)
    r <- getAttribute state v k
    pure $ r === Just v

prop_put_overwrite = forAll (elements simpsons) $ \v ->
  testIO $ do
    state <- newTableState g
    put state v (Attribute k v)
    set state v [Attribute ki 10]
    r <- get state v
    pure $ r === Just [Attribute ki 10]

prop_put_with_ok = forAll (elements simpsons) $ \v ->
  testIO $ do
    state <- newTableState g
    put state v (Attribute k v)
    r <- putWith state v (Attribute k v) (Exists k)
    pure $ r === True

prop_put_with_fail = forAll (elements simpsons) $ \v ->
  testIO $ do
    state <- newTableState g
    put state v (Attribute k v)
    r <- putWith state v (Attribute k v) (NotExists k)
    pure $ r === False

prop_update_with_contains_ok = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute ks [s])
    r <- updateWith state s [Attribute ks [m]] (Contains ks [s])
    z <- get state s
    pure $ (r, z) === (True, Just [Attribute ks [s, m]])

prop_update'_notcontains_ok = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute ks [s])
    r <- updateWith state s [Attribute ks [m]] (NotContains ks [m])
    z <- get state s
    pure $ (r, z) === (True, Just [Attribute ks [s, m]])

prop_set_with_notcontains_fail = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute ks [s])
    r <- setWith state s [Attribute ks [m]] (NotContains ks [s])
    pure $ r === False

prop_delete = forAll (elements simpsons) $ \v ->
  testIO $ do
    state <- newTableState g
    put state v (Attribute k v)
    delete state v
    r <- get state v
    pure $ r === Nothing

prop_delete_set = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute ks [s, m])
    set state s [Attribute k s]
    deleteFromSet state s ks m
    r <- get state s
    pure $ r === Just [Attribute k s, Attribute ks [s]]

prop_lookup_value_nothing as =
  lookupValue as k === Nothing

prop_lookup_value a@(Attribute key v) =
  lookupValue [a] key === Just v

prop_set = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute k s)
    set state s [Attribute k m]
    r <- get state s
    pure $ r === Just [Attribute k m]

prop_set_multi = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    let as = [Attribute k m, Attribute (StringKey "test2") s]
    set state s as
    r <- get state s
    pure $ r === Just as

prop_set_delete_multi = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute ks [s, m])
    set state s [Attribute k s]
    set state s [Attribute (StringSetKey "test2") [m]]
    deleteFromSet state s ks m
    r <- get state s
    pure $ r === Just [Attribute (StringSetKey "test2") [m], Attribute k s, Attribute ks [s]]

prop_set_with_value = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute k s)
    z <- setWith state s [Attribute k m] (ExistsValue k s)
    r <- get state s
    pure $ (z, r) === (True, Just [Attribute k m])

prop_set_with_value_fail = forAll ((,) <$> elements simpsons <*> elements muppets) $ \(s, m) ->
  testIO $ do
    state <- newTableState g
    put state s (Attribute k s)
    z <- setWith state s [Attribute k m] (ExistsValue k m)
    r <- get state s
    pure $ (z, r) === (False, Just [Attribute k s])

return []
tests = $quickCheckAll
