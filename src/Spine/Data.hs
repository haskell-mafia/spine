{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE BangPatterns #-}
module Spine.Data (
    Schema (..)
  , Table (..)
  , TableName (..)
  , ThroughputPorridge (..)
  , Throughput (..)
  , ThroughputRange (..)
  , ItemKey (..)
  , Key (..)
  , DecodeError (..)
  , renderKey
  , toEncoding
  , toValidSetEncoding
  , fromEncoding_
  , fromEncoding
  , toItemEncoding
  , fromItemEncoding_
  , fromItemEncoding
  , renderTime
  , parseTime
  , renderItemKey
  , checkThroughput
  , desiredThroughput
  , isJustRight
  ) where

import           Data.ByteString (ByteString)

import           Control.Lens ((.~), (^?), (^.), ix, _Just)

import           Data.HashMap.Strict (HashMap)
import qualified Data.Text as T
import           Data.Time (UTCTime, parseTimeM, formatTime)
import           Data.Time.Locale.Compat (defaultTimeLocale)

import           GHC.Show (appPrec, appPrec1)

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



data ThroughputPorridge =
    TooLow ThroughputRange
  | TooHigh ThroughputRange
  | JustRight Natural
    deriving (Eq, Show)

-- |
-- Throughput configures the table throughput but allows for external scaling
-- events to be applied.
--
-- The expected semantics for throughput is that any table that has less than
-- the minumum will be increased to the minimum, any table that has more than
-- the maximum will be decreased to the maximum.
--
data Throughput =
  Throughput {
      readThroughput :: ThroughputRange
    , writeThroughput :: ThroughputRange
    } deriving (Eq, Show)

data ThroughputRange =
  ThroughputRange {
      minThroughput :: Natural
    , maxThroughput :: Natural
    } deriving (Eq, Show)

data ItemKey a where
  ItemIntKey :: Text -> ItemKey Int
  ItemStringKey :: Text -> ItemKey Text
  ItemBinaryKey :: Text -> ItemKey ByteString

instance Eq (ItemKey a) where
  (ItemIntKey k) == (ItemIntKey k') =
    (k == k')
  (ItemStringKey k) == (ItemStringKey k') =
    (k == k')
  (ItemBinaryKey k) == (ItemBinaryKey k') =
    (k == k')
  _ == _ =
    False

instance Show (ItemKey a) where
  showsPrec p (ItemIntKey k) =
    showParen (p > appPrec) $
      showString "ItemIntKey " . showsPrec appPrec1 k
  showsPrec p (ItemStringKey k) =
    showParen (p > appPrec) $
      showString "ItemStringKey " . showsPrec appPrec1 k
  showsPrec p (ItemBinaryKey k) =
    showParen (p > appPrec) $
      showString "ItemBinaryKey " . showsPrec appPrec1 k

data Key a where
  IntKey :: Text -> Key Int
  IntSetKey :: Text -> Key [Int]
  StringKey :: Text -> Key Text
  StringSetKey :: Text -> Key [Text]
  StringListKey :: Text -> Key [Text]
  BinaryKey :: Text -> Key ByteString
  BinarySetKey :: Text -> Key [ByteString]
  TimeKey :: Text -> Key UTCTime
  BoolKey :: Text -> Key Bool
  NullKey :: Text -> Key ()
  MapKey :: Text -> Key (HashMap Text D.AttributeValue)


instance Eq (Key a) where
  (IntKey k) == (IntKey k') =
    (k == k')
  (IntSetKey k) == (IntSetKey k') =
    (k == k')
  (StringKey k) == (StringKey k') =
    (k == k')
  (StringSetKey k) == (StringSetKey k') =
    (k == k')
  (StringListKey k) == (StringListKey k') =
    (k == k')
  (BinaryKey k) == (BinaryKey k') =
    (k == k')
  (BinarySetKey k) == (BinarySetKey k') =
    (k == k')
  (TimeKey k) == (TimeKey k') =
    (k == k')
  (BoolKey k) == (BoolKey k') =
    (k == k')
  (NullKey k) == (NullKey k') =
    (k == k')
  (MapKey k) == (MapKey k') =
    (k == k')
  _ ==
    _ = False

instance Show (Key a) where
  showsPrec p (IntKey k) =
    showParen (p > appPrec) $
      showString "IntKey " . showsPrec appPrec1 k
  showsPrec p (IntSetKey k) =
    showParen (p > appPrec) $
      showString "IntSetKey " . showsPrec appPrec1 k
  showsPrec p (StringKey k) =
    showParen (p > appPrec) $
      showString "StringKey " . showsPrec appPrec1 k
  showsPrec p (StringSetKey k) =
    showParen (p > appPrec) $
      showString "StringSetKey " . showsPrec appPrec1 k
  showsPrec p (StringListKey k) =
    showParen (p > appPrec) $
      showString "StringListKey " . showsPrec appPrec1 k
  showsPrec p (BinaryKey k) =
    showParen (p > appPrec) $
      showString "BinaryKey " . showsPrec appPrec1 k
  showsPrec p (BinarySetKey k) =
    showParen (p > appPrec) $
      showString "BinarySetKey " . showsPrec appPrec1 k
  showsPrec p (TimeKey k) =
    showParen (p > appPrec) $
      showString "TimeKey " . showsPrec appPrec1 k
  showsPrec p (BoolKey k) =
    showParen (p > appPrec) $
      showString "BoolKey " . showsPrec appPrec1 k
  showsPrec p (NullKey k) =
    showParen (p > appPrec) $
      showString "NullKey " . showsPrec appPrec1 k
  showsPrec p (MapKey k) =
    showParen (p > appPrec) $
      showString "MapKey " . showsPrec appPrec1 k

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
    StringListKey v ->
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

data DecodeError =
    CouldNotDecodeInt Text
  | CouldNotDecodeIntSet [Text]
  | CouldNotDecodeTime Text
    deriving (Eq, Show)

fromEncoding_ :: Key a -> (HashMap Text D.AttributeValue) -> Maybe a
fromEncoding_ k l =
  case fromEncoding k l of
    Left _ ->
      Nothing
    Right m ->
      m

fromEncoding :: Key a -> (HashMap Text D.AttributeValue) -> Either DecodeError (Maybe a)
fromEncoding k l =
  case k of
    IntKey v -> do
      case l ^? ix v . D.avN . _Just of
        Nothing ->
          pure Nothing
        Just t ->
          maybe (Left $ CouldNotDecodeInt t) (pure . pure) .
            readMaybe . T.unpack $ t
    IntSetKey v ->
      case l ^? ix v . D.avNS of
        Nothing ->
          pure Nothing
        Just ts ->
          maybe (Left $ CouldNotDecodeIntSet ts) (pure . pure) $
            traverse (readMaybe . T.unpack) ts
    StringKey v ->
      pure $ l ^? ix v . D.avS . _Just
    StringSetKey v ->
      pure $ l ^? ix v . D.avSS
    StringListKey v -> pure $ do
       xs <- (l ^? ix v . D.avL)
       forM xs $ \x -> x ^. D.avS
    BinaryKey v ->
      pure $ l ^? ix v . D.avB . _Just
    BinarySetKey v ->
      pure $ l ^? ix v . D.avBS
    TimeKey v ->
      case l ^? ix v . D.avS . _Just of
        Nothing ->
          pure Nothing
        Just t ->
          maybe (Left $ CouldNotDecodeTime t) (pure . pure) .
            parseTime $ t
    BoolKey v ->
      pure $ l ^? ix v . D.avBOOL . _Just
    NullKey v ->
      pure $ l ^? ix v . D.avNULL . _Just >>= bool Nothing (Just ())
    MapKey v ->
      pure $ l ^? ix v . D.avM

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
    StringListKey v ->
      (v, D.attributeValue & D.avL .~ fmap (\x -> D.attributeValue & D.avS .~ Just x) a)
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

toValidSetEncoding :: Key [a] -> [a] -> [(Text, D.AttributeValue)]
toValidSetEncoding k a =
  let
    check x f =
      case x of
        [] ->
          []
        _ ->
          f
  in
    case k of
      IntSetKey v ->
        check a $
          [(v, D.attributeValue & D.avNS .~ (fmap renderIntegral a))]
      StringSetKey v ->
        check a $
          [(v, D.attributeValue & D.avSS .~ a)]
      StringListKey v ->
        check a $
          [(v, D.attributeValue & D.avL .~ fmap (\x -> D.attributeValue & D.avS .~ Just x) a)]
      BinarySetKey v ->
        check a $
          [(v, D.attributeValue & D.avBS .~ a)]

fromItemEncoding_ :: ItemKey a -> (HashMap Text D.AttributeValue) -> Maybe a
fromItemEncoding_ k l =
  case fromItemEncoding k l of
    Left _ ->
      Nothing
    Right m ->
      m

fromItemEncoding :: ItemKey a -> (HashMap Text D.AttributeValue) -> Either DecodeError (Maybe a)
fromItemEncoding k l =
  case k of
    ItemIntKey v ->
      case l ^? ix v . D.avN . _Just of
        Nothing ->
          pure Nothing
        Just t ->
          maybe (Left $ CouldNotDecodeInt t) (pure . pure) .
            readMaybe . T.unpack $ t
    ItemStringKey v ->
      pure $ l ^? ix v . D.avS . _Just
    ItemBinaryKey v ->
      pure $ l ^? ix v . D.avB . _Just

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

checkThroughput :: Maybe Natural -> ThroughputRange -> ThroughputPorridge
checkThroughput actual range =
  case actual of
    Nothing ->
      TooLow range
    Just v ->
      case v of
        _ | v < minThroughput range ->
          TooLow range
        _ | v > maxThroughput range ->
          TooHigh range
        _ ->
          JustRight v

desiredThroughput :: ThroughputPorridge -> Natural
desiredThroughput porridge =
  case porridge of
    TooLow range ->
      minThroughput range
    TooHigh range ->
      maxThroughput range
    JustRight v ->
      v

isJustRight :: ThroughputPorridge -> Bool
isJustRight  porridge =
  case porridge of
    TooLow _ ->
      False
    TooHigh _ ->
      False
    JustRight _ ->
      True
