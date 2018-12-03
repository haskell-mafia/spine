{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.IO.Spine.Schema where

import Control.Monad.Trans.Either (runEitherT)
import Spine.Data
import Spine.Schema
import Test.Mismi (testAWS)
import Test.QuickCheck
import Test.Spine.Schema

testTableName :: TableName
testTableName =
  TableName "spine.test.schema"

testTableNameTwo :: TableName
testTableNameTwo =
  TableName "spine.test.schema.two"

testTableNameThree :: TableName
testTableNameThree =
  TableName "spine.test.schema.three"

testTableNameFour :: TableName
testTableNameFour =
  TableName "spine.test.schema.four"

testTableNameFive :: TableName
testTableNameFive =
  TableName "spine.test.schema.five"

testThroughput :: Throughput
testThroughput =
  let
    one = ThroughputRange 1 2
  in
    Throughput one one

testThroughputTwo :: Throughput
testThroughputTwo =
  let
    two = ThroughputRange 2 2
  in
    Throughput two two

testThroughputThree :: Throughput
testThroughputThree =
  let
    three = ThroughputRange 3 3
  in
    Throughput three three

prop_initialise_destroy = once . testAWS .
  withClean (Schema []) (pure ()) $
    pure $ True === True

prop_primary_key_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey-foo") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaKeysMismatch testTableName)

prop_sort_key_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key-foo") [] testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaKeysMismatch testTableName)

prop_primary_key_type_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemIntKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaAttributeMismatch testTableName)

prop_sort_key_type_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemIntKey "spine-sort-key") [] testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaAttributeMismatch testTableName)

prop_attribute_mismatch_order = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableNameTwo (ItemStringKey "spine-b") (Just $ ItemStringKey "spine-a") [] testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableNameTwo (ItemStringKey "spine-b") (Just $ ItemStringKey "spine-a") [] testThroughput]
  pure $ (a, b) === (Right (), Right ())

prop_idempotent = once . testAWS $ do
  let
    schema = Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  a <- runEitherT . initialise $ schema
  b <- runEitherT . initialise $ schema
  pure $ (a, b) === (Right (), Right ())

prop_throughput = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") [] $ testThroughputTwo]
  pure $ (a, b) === (Right (), Right ())

prop_secondary = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [
      Table
        testTableNameThree
        (ItemStringKey "spine-pkey")
        Nothing
        [SecondaryIndex (IndexName "spine-index1") (ItemStringKey "spine-b") Nothing Nothing testThroughput]
        testThroughput]
  b <- runEitherT . initialise $
    Schema [
      Table
        testTableNameThree
        (ItemStringKey "spine-pkey")
        Nothing
        [SecondaryIndex (IndexName "spine-index1") (ItemStringKey "spine-b") Nothing Nothing testThroughputThree]
        testThroughput]
  pure $ (a, b) === (Right (), Right ())

prop_secondary_compat = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [
      Table
        testTableNameFour
        (ItemStringKey "spine-pkey")
        (Just $ ItemStringKey "spine-sort-key")
        []
        testThroughputTwo]
  b <- runEitherT . initialise $
    Schema [
      Table
        testTableNameFour
        (ItemStringKey "spine-pkey")
        (Just $ ItemStringKey "spine-sort-key")
        [SecondaryIndex (IndexName "spine-index1") (ItemStringKey "spine-b") Nothing Nothing testThroughput]
        testThroughputTwo]
  c <- runEitherT . initialise $
    Schema [
      Table
        testTableNameFour
        (ItemStringKey "spine-pkey")
        (Just $ ItemStringKey "spine-sort-key")
        [SecondaryIndex (IndexName "spine-index1") (ItemStringKey "spine-b") Nothing Nothing testThroughputTwo]
        testThroughputTwo]
  pure $ (a, b, c) === (Right (), Right (), Right ())

prop_secondary_index_sort_key = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [
      Table
        testTableNameFive
        (ItemIntKey "spine-pkey")
        Nothing
        [SecondaryIndex (IndexName "spine-index2") (ItemStringKey "spine-b") (Just $ ItemIntKey "spine-sort-int") Nothing testThroughput]
        testThroughputTwo]
  pure $ a === Right ()

return []
tests = $quickCheckAll
