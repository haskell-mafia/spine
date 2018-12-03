{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.IO.Spine.Data where

import           Control.Lens ((.~), (^.), (&))
import           Control.Monad.IO.Class (liftIO)
import qualified Data.HashMap.Strict as H
import           Data.Semigroup ((<>))
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Time (UTCTime, getCurrentTime)
import           Hedgehog.Corpus
import qualified Mismi.Amazonka as A
import qualified Network.AWS.DynamoDB as D
import           Spine.Data
import           Spine.P
import           Test.Mismi (testAWS)
import           Test.QuickCheck
import           Test.Spine.Schema

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

kStringTime :: Key UTCTime
kStringTime =
  TimeKey "textx"

kStringSet :: Key [Text]
kStringSet =
  StringSetKey "textsetx"

kStringEmptySet :: Key [Text]
kStringEmptySet =
  StringSetKey "textsetx_empty"

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

kList :: Key [Text]
kList =
  StringListKey "listx"

schema :: Schema
schema =
  Schema [Table testTableName item Nothing [] testThroughput]

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
      s = renderIntegral n
      listt = [s, s, renderIntegral $ n + 1, s]

    -- put items
    void . A.send $ D.putItem (renderTableName testTableName)
      & D.piItem .~ H.fromList ([
          toItemEncoding item m
        , toEncoding kInt n
        , toEncoding kIntSet set
        , toEncoding kString s
        , toEncoding kStringSet (fmap renderIntegral set)
        , toEncoding kBinary bytes
        , toEncoding kBinarySet [bytes]
        , toEncoding kTime now
        , toEncoding kBool b
        , toEncoding kNull ()
        , toEncoding kMap mapp
        , toEncoding kList listt
        ] <> toValidSetEncoding kStringEmptySet [])

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
        , renderKey kList
        ])
      & D.giConsistentRead .~ Just False

    let
      attrs :: A.HashMap Text D.AttributeValue
      attrs = r ^. D.girsItem

      handler name e =
        case e of
          Left d ->
            fail $ "[" <> name <> "] decode error: " <> show d
          Right Nothing ->
            fail $ "[" <> name <> "] missing."
          Right (Just a) ->
            pure a

    ri <- handler "item" $ fromItemEncoding item attrs
    rn <- handler "kInt" $ fromEncoding kInt attrs
    rns <- handler "kIntSet" $ fromEncoding kIntSet attrs
    rs <- handler "kString" $ fromEncoding kString attrs
    rss <- handler "kStringSet" $ fromEncoding kStringSet attrs
    rby <- handler "kBinary" $ fromEncoding kBinary attrs
    rbys <- handler "kBinarySet" $ fromEncoding kBinarySet attrs
    rb <- handler "kBool" $ fromEncoding kBool attrs
    rt <- handler "kTime" $ fromEncoding kTime attrs
    ru <- handler "kNull" $ fromEncoding kNull attrs
    rm <- handler "kMap" $ fromEncoding kMap attrs
    rl <- handler "kList" $ fromEncoding kList attrs

    let
      decode = fromEncoding kStringTime attrs

    pure $
      (ri, rn, rns, rs, rss, rby, rbys, rb, ru, rm, rt, decode, rl)
      ===
      (m, n, set, s, fmap renderIntegral set, bytes, [bytes], b, (), mapp, rt, Left $ CouldNotDecodeTime s, listt)

return []
tests = $quickCheckAll
