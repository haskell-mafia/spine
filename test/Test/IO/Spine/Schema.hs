{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.IO.Spine.Schema where

import           Spine.Data
import           Spine.Schema

import           P

import           Test.Mismi (testAWS)
import           Test.Spine.Schema
import           Test.QuickCheck

import           X.Control.Monad.Trans.Either (runEitherT)

testTableName :: TableName
testTableName =
  TableName "spine.test.schema"

testThroughput :: Throughput
testThroughput =
  Throughput 1 1

prop_initialise_destroy = once . testAWS .
  withClean (Schema []) (pure ()) $
    pure $ True === True

prop_primary_key_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey-foo") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaKeysMismatch testTableName)

prop_sort_key_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key-foo") testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaKeysMismatch testTableName)

prop_primary_key_type_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemIntKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaAttributeMismatch testTableName)

prop_sort_key_type_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemIntKey "spine-sort-key") testThroughput]
  pure $ (a, b) === (Right (), Left $ SchemaAttributeMismatch testTableName)

prop_idempotent = once . testAWS $ do
  let
    schema = Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  a <- runEitherT . initialise $ schema
  b <- runEitherT . initialise $ schema
  pure $ (a, b) === (Right (), Right ())

prop_throughput = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (ItemStringKey "spine-pkey") (Just $ ItemStringKey "spine-sort-key") $ Throughput 2 2]
  pure $ (a, b) === (Right (), Right ())

return []
tests = $quickCheckAll
