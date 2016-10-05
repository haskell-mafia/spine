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
  TableName "spine.test"

testThroughput :: Throughput
testThroughput =
  Throughput 1 1

prop_initialise_destroy = once . testAWS .
  withClean (Schema []) (pure ()) $
    pure $ True === True

prop_primary_key_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey-foo") (Just $ StringKey "spine-sort-key") testThroughput]
  pure $ (a, b) === (Right (), Left SchemaKeysMismatch)

prop_sort_key_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key-foo") testThroughput]
  pure $ (a, b) === (Right (), Left SchemaKeysMismatch)

prop_primary_key_type_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (IntKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  pure $ (a, b) === (Right (), Left SchemaAttributeMismatch)

prop_sort_key_type_failure = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ IntKey "spine-sort-key") testThroughput]
  pure $ (a, b) === (Right (), Left SchemaAttributeMismatch)

prop_idempotent = once . testAWS $ do
  let
    schema = Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  a <- runEitherT . initialise $ schema
  b <- runEitherT . initialise $ schema
  pure $ (a, b) === (Right (), Right ())

prop_throughput = once . testAWS $ do
  a <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") testThroughput]
  b <- runEitherT . initialise $
    Schema [Table testTableName (StringKey "spine-pkey") (Just $ StringKey "spine-sort-key") $ Throughput 2 2]
  pure $ (a, b) === (Right (), Right ())

return []
tests = $quickCheckAll
