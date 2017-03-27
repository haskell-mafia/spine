{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Spine.Arbitrary where

import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as H

import           Disorder.Core
import           Disorder.Corpus

import qualified Network.AWS.DynamoDB as D

import           P

import           Spine.Data
import           Spine.Memory

import           Test.QuickCheck
import           Test.QuickCheck.Instances ()

instance Arbitrary Attribute where
  arbitrary =
    genAttribute

genHashMap :: Gen (HashMap Text D.AttributeValue)
genHashMap = do
  ls <- listOf genAttribute
  pure . H.fromList . with ls $ \(Attribute k v) ->
    toEncoding k v

genAttribute :: Gen Attribute
genAttribute =
  oneof [
      Attribute <$> (IntKey <$> elements simpsons) <*> arbitrary
    , Attribute <$> (IntSetKey <$> elements cooking) <*> arbitrary
    , Attribute <$> (StringKey <$> elements muppets) <*> arbitrary
    , Attribute <$> (StringSetKey <$> elements southpark) <*> arbitrary
    , Attribute <$> (BinaryKey <$> elements simpsons) <*> arbitrary
    , Attribute <$> (BinarySetKey <$> elements viruses) <*> arbitrary
    , Attribute <$> (TimeKey <$> elements colours) <*> arbitrary
    , Attribute <$> (BoolKey <$> elements weather) <*> arbitrary
    , Attribute <$> (NullKey <$> elements waters) <*> arbitrary
    , Attribute <$> (MapKey <$> elements waters) <*> (smaller genHashMap)
    ]
