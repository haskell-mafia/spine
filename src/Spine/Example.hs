{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Spine.Example (
    kThing
  , cThing
  ) where

import           Spine.Data
import           Spine.Schema

import           P

kThing :: Key Text
kThing =
  StringKey "thing"

cThing :: KeyCodec Text
cThing =
  cText kThing
