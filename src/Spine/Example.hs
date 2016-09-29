{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Spine.Example (
    kThing
  ) where

import           Spine.Data

import           P

kThing :: Key Text
kThing =
  StringKey "thing"
