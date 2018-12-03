{-# LANGUAGE OverloadedStrings #-}
module Spine.Example (
    schema
  , table
  , kThing
  ) where

import Data.Text (Text)
import Spine.Data

schema :: Schema
schema =
  Schema
    [table]

table :: Table
table =
  Table (TableName "spine.local") kThing Nothing [] (Throughput (ThroughputRange 2 2) (ThroughputRange 1 1))

kThing :: ItemKey Text
kThing =
  ItemStringKey "thing"
