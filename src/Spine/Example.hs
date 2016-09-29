{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Spine.Example (
    kThing
  , cThingy
  ) where

import           Spine.Data

import           P

kThing :: Key Text
kThing =
  StringKey "thing"

--type CodecAWS = Codec AWS

type KeyCodec k = Codec (Key k) k k

cThingy :: KeyCodec Text
cThingy =
  Codec {
      put =
        id
    , get =
        Just
    }
