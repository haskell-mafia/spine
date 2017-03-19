{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Spine.Arbitrary where

import           Disorder.Corpus

import           Spine.Data
import           Spine.Memory

import           Test.QuickCheck
import           Test.QuickCheck.Instances ()

instance Arbitrary Attribute where
  arbitrary =
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
      ]
