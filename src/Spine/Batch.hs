{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
module Spine.Batch (
    consume
  , write
  ) where

import           Control.Lens ((.~), (^.))

import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as H

import           Mismi (AWS)
import qualified Mismi.Amazonka as A

import qualified Network.AWS.DynamoDB as D

import           P

consume :: D.BatchGetItem -> AWS (HashMap Text [HashMap Text D.AttributeValue])
consume action =
  let
    go actiony !thingy = do
      r <- A.send actiony
      let
        unprocessed =  r ^. D.bgirsUnprocessedKeys
        responses = r ^. D.bgirsResponses
      case unprocessed == H.empty of
        True ->
          pure $ H.unionWith (<>) responses thingy
        False ->
          let
            thing =
               D.batchGetItem
                 & D.bgiRequestItems .~ unprocessed
          in
            go thing $ H.unionWith (<>) responses thingy
  in
   go action H.empty


write:: D.BatchWriteItem -> AWS ()
write action =
  let
    go actiony = do
      r <- A.send actiony
      let
        unprocessed =  r ^. D.bwirsUnprocessedItems
      case unprocessed == H.empty of
        True ->
          pure ()
        False ->
          go $ D.batchWriteItem
            & D.bwiRequestItems .~ unprocessed
  in
   go action
