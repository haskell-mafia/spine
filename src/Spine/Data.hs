{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ExistentialQuantification #-}
module Spine.Data (
    Schema (..)
  , Table (..)
  , TableName (..)
  , Throughput (..)
  , ItemKey (..)
  , Key (..)
  , renderKey
  , toEncoding
  , fromEncoding
  , toItemEncoding
  , renderTime
  , parseTime
  , renderItemKey
  ) where

import           Data.ByteString (ByteString)

import           Control.Lens ((.~), (^?), ix, _Just)

import           Data.HashMap.Strict (HashMap)
import qualified Data.Text as T
import           Data.Time (UTCTime, parseTimeM, formatTime)
import           Data.Time.Locale.Compat (defaultTimeLocale)

import           Numeric.Natural (Natural)

import           P

import qualified Network.AWS.DynamoDB as D

newtype Schema =
  Schema {
      schemaTables :: [Table]
    }

data Table = forall a b.
  Table {
      tableName :: TableName
    , tablePartitionKey :: ItemKey a
    , tableSortKey :: Maybe (ItemKey b)
    , tableThroughput :: Throughput
    }

newtype TableName =
  TableName {
      renderTableName :: Text
    } deriving (Eq, Show)

data Throughput =
  Throughput {
      readThroughput :: Natural
    , writeThroughput :: Natural
    } deriving (Eq, Show)

data ItemKey a where
  ItemIntKey :: Text -> ItemKey Int
  ItemStringKey :: Text -> ItemKey Text
  ItemBinaryKey :: Text -> ItemKey ByteString

data Key a where
  IntKey :: Text -> Key Int
  IntSetKey :: Text -> Key [Int]
  StringKey :: Text -> Key Text
  StringSetKey :: Text -> Key [Text]
  BinaryKey :: Text -> Key ByteString
  BinarySetKey :: Text -> Key [ByteString]
  TimeKey :: Text -> Key UTCTime
  BoolKey :: Text -> Key Bool
  NullKey :: Text -> Key ()
  MapKey :: Text -> Key (HashMap Text D.AttributeValue)

renderKey :: Key a -> Text
renderKey k =
  case k of
    IntKey v ->
      v
    IntSetKey v ->
      v
    StringKey v ->
      v
    StringSetKey v ->
      v
    BinaryKey v ->
      v
    BinarySetKey v ->
      v
    TimeKey v ->
      v
    BoolKey v ->
      v
    NullKey v ->
      v
    MapKey v ->
      v

fromEncoding :: Key a -> (HashMap Text D.AttributeValue) -> Maybe a
fromEncoding k l =
  case k of
    IntKey v ->
      l ^? ix v . D.avN . _Just >>= readMaybe . T.unpack
    IntSetKey v ->
      l ^? ix v . D.avNS >>= traverse (readMaybe . T.unpack)
    StringKey v ->
      l ^? ix v . D.avS . _Just
    StringSetKey v ->
      l ^? ix v . D.avSS
    BinaryKey v ->
      l ^? ix v . D.avB . _Just
    BinarySetKey v ->
      l ^? ix v . D.avBS
    TimeKey v ->
      l ^? ix v . D.avS . _Just >>= parseTime
    BoolKey v ->
      l ^? ix v . D.avBOOL . _Just
    NullKey v ->
      l ^? ix v . D.avNULL . _Just >>= bool Nothing (Just ())
    MapKey v ->
      l ^? ix v . D.avM

toEncoding :: Key a -> a -> (Text, D.AttributeValue)
toEncoding k a =
  case k of
    IntKey v ->
      (v, D.attributeValue & D.avN .~ Just (renderIntegral a))
    IntSetKey v ->
      (v, D.attributeValue & D.avNS .~ (fmap renderIntegral a))
    StringKey v ->
      (v, D.attributeValue & D.avS .~ Just a)
    StringSetKey v ->
      (v, D.attributeValue & D.avSS .~ a)
    BinaryKey v ->
      (v, D.attributeValue & D.avB .~ Just a)
    BinarySetKey v ->
      (v, D.attributeValue & D.avBS .~ a)
    TimeKey v ->
      (v, D.attributeValue & D.avS .~ Just (renderTime a))
    BoolKey v ->
      (v, D.attributeValue & D.avBOOL .~ Just a)
    NullKey v ->
      (v, D.attributeValue & D.avNULL .~ Just True)
    MapKey v ->
      (v, D.attributeValue & D.avM .~ a)

toItemEncoding :: ItemKey a -> a -> (Text, D.AttributeValue)
toItemEncoding i a =
  case i of
    ItemIntKey v ->
      (v, D.attributeValue & D.avN .~ Just (renderIntegral a))
    ItemStringKey v ->
      (v, D.attributeValue & D.avS .~ Just a)
    ItemBinaryKey v ->
      (v, D.attributeValue & D.avB .~ Just a)

renderTime :: UTCTime -> Text
renderTime =
  T.pack . formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S"

parseTime :: Text -> Maybe UTCTime
parseTime =
  parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S" . T.unpack

renderItemKey :: ItemKey a -> Text
renderItemKey a =
  case a of
    ItemIntKey v ->
      v
    ItemStringKey v ->
      v
    ItemBinaryKey v ->
      v
