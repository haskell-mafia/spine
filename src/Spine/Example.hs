{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Spine.Example (
    kThing
  ) where

import           Spine.Data

import           P
import           System.IO

kThing :: Key Text
kThing =
  StringKey "thing"


thingy :: Codec (Text) IO
thingy =
  Codec {
      put = kThing
    , get = kThing
    }
