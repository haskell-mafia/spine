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

type KeyCodec k v = Codec (Key k) k v

cThingy :: KeyCodec Text Text
cThingy =
  Codec {
      put =
        id
    , get =
        Just
    }
