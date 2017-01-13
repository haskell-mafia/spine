{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.IO.Spine.Data where

import           Control.Lens ((.~), (^.))
import           Control.Monad.IO.Class (liftIO)

import qualified Data.HashMap.Strict as H
import           Data.Time (UTCTime, getCurrentTime)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           Disorder.Corpus

import qualified Network.AWS.DynamoDB as D

import qualified Mismi.Amazonka as A

import           P

import           Spine.Data

import           Test.Mismi (testAWS)
import           Test.Spine.Schema
import           Test.QuickCheck


testTableName :: TableName
testTableName =
  TableName "spine.test.data"

testThroughput :: Throughput
testThroughput =
  let one = ThroughputRange 1 1 in Throughput one one

item :: ItemKey Text
item =
  ItemStringKey "spinepkey"

kInt :: Key Int
kInt =
  IntKey "intx"

kIntSet :: Key [Int]
kIntSet =
  IntSetKey "intsetx"

kString :: Key Text
kString =
  StringKey "textx"

kStringSet :: Key [Text]
kStringSet =
  StringSetKey "textsetx"

kBinary :: Key A.ByteString
kBinary =
  BinaryKey "binaryx"

kBinarySet :: Key [A.ByteString]
kBinarySet =
  BinarySetKey "binarysetx"

kTime :: Key UTCTime
kTime =
  TimeKey "timex"

kBool :: Key Bool
kBool =
  BoolKey "boolx"

kNull :: Key ()
kNull =
  NullKey "nullx"

kMap :: Key (H.HashMap Text D.AttributeValue)
kMap =
  MapKey "mapx"

schema :: Schema
schema =
  Schema [Table testTableName item Nothing testThroughput]

delete :: Text -> A.AWS ()
delete t =
  void . A.send $ D.deleteItem (renderTableName testTableName)
    & D.diKey .~ H.fromList [
        toItemEncoding item t
      ]

prop_encodings n b = forAll (elements muppets) $ \m ->
  once . testAWS . withClean (schema) (delete m) $ do
    now <- liftIO getCurrentTime
    let
      set = [n, n + 1]
      bytes = T.encodeUtf8 m
      mapp = H.fromList [toEncoding kInt n, toEncoding kString (renderIntegral n)]

    -- put items
    void . A.send $ D.putItem (renderTableName testTableName)
      & D.piItem .~ H.fromList [
          toItemEncoding item m
        , toEncoding kInt n
        , toEncoding kIntSet set
        , toEncoding kString (renderIntegral n)
        , toEncoding kStringSet (fmap renderIntegral set)
        , toEncoding kBinary bytes
        , toEncoding kBinarySet [bytes]
        , toEncoding kTime now
        , toEncoding kBool b
        , toEncoding kNull ()
        , toEncoding kMap mapp
        ]

    -- get items
    r <- A.send $ D.getItem (renderTableName testTableName)
      & D.giKey .~ H.fromList [
          toItemEncoding item m
        ]
      & D.giProjectionExpression .~ Just (T.intercalate "," [
          renderItemKey item
        , renderKey kInt
        , renderKey kIntSet
        , renderKey kString
        , renderKey kStringSet
        , renderKey kBinary
        , renderKey kBinarySet
        , renderKey kTime
        , renderKey kBool
        , renderKey kNull
        , renderKey kMap
        ])
      & D.giConsistentRead .~ Just False

    let
      attrs :: A.HashMap Text D.AttributeValue
      attrs = r ^. D.girsItem

    ri <- fromMaybeM (fail "item failed") $ fromItemEncoding item attrs
    rn <- fromMaybeM (fail "kInt failed") $ fromEncoding kInt attrs
    rns <- fromMaybeM (fail "kIntSet failed") $ fromEncoding kIntSet attrs
    rs <- fromMaybeM (fail "kString failed") $ fromEncoding kString attrs
    rss <- fromMaybeM (fail "kStringSet failed") $ fromEncoding kStringSet attrs
    rby <- fromMaybeM (fail "kBinary failed") $ fromEncoding kBinary attrs
    rbys <- fromMaybeM (fail "kBinarySet failed") $ fromEncoding kBinarySet attrs
    rb <- fromMaybeM (fail "kBool failed") $ fromEncoding kBool attrs
    rt <- fromMaybeM (fail "kTime failed") $ fromEncoding kTime attrs
    ru <- fromMaybeM (fail "kNull failed") $ fromEncoding kNull attrs
    rm <- fromMaybeM (fail "kMap failed") $ fromEncoding kMap attrs

    pure $
      (ri, rn, rns, rs, rss, rby, rbys, rb, ru, rm, rt)
      ===
      (m, n, set, renderIntegral n, fmap renderIntegral set, bytes, [bytes], b, (), mapp, rt)

return []
tests = $quickCheckAll
