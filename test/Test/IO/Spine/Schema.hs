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

schema :: Schema
schema =
  Schema
    "spine.local"
    []

prop_initialise_destroy = once . testAWS .
  withClean schema (destroy schema) $
    pure $ True === True

return []
tests = $quickCheckAll
