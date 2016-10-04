{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Spine.Example (
    schema
  , table
  , kThing
  ) where

import           Spine.Data

import           P

schema :: Schema
schema =
  Schema
    [table]

table :: Table
table =
  Table (TableName "spine.local") kThing Nothing (Throughput 2 1)

kThing :: Key Text
kThing =
  StringKey "thing"
