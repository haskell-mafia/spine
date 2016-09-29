{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ExistentialQuantification #-}
module Spine.Data (
    Schema (..)
  , Table (..)
  , Throughput (..)
  , Key (..)
  , renderPrimaryKey
  , renderSortKey
  , primaryKeyType
  , sortKeyType
  , tableToCreate
  , tableToSchemaElement
  , tableToAttributeDefintions
  , toThroughput

  , Codec' (..)
  , Codec (..)
  , CodecX (..)
  ) where

import           Control.Lens ((.~))

import           Data.List.NonEmpty (NonEmpty (..))

import           Numeric.Natural (Natural)

import           P

import qualified Network.AWS.DynamoDB as D


data Schema =
  Schema {
      schemaPrefix :: Text
    , schemaTables :: [Table]
    }

data Table = forall a b.
  Table {
      tableName :: Text
    , tablePrimaryKey :: Key a
    , tableSortKey :: Maybe (Key b)
    , tableThroughput :: Throughput
    }

data Throughput =
  Throughput {
      readThroughput :: Natural
    , writeThroughput :: Natural
    } deriving (Eq, Show)

data Key a where
  IntKey :: Text -> Key Int
  StringKey :: Text -> Key Text

data Codec' f g a b =
  Codec' { codecEncoder :: f a , codecDecoder :: g b }

data Codec f a v =
  Codec {
      put :: a -> v
    , get :: v -> Maybe a
    }

-- *-simple style
class CodecX a where
  putx :: Key a -> a -> m ()
  getx :: Key a -> m (Maybe a)

-- type Codec e v = Tuple (v -> String) (String -> Either e v)
--data Codec a where

{-
data Codec (Key a) where
  Encode :: a -> Codec a
  Decode :: a -> (Maybe (Codec a))
-}


renderPrimaryKey :: Table -> Text
renderPrimaryKey (Table _ v _ _) =
  renderKey v

renderSortKey :: Table -> Maybe Text
renderSortKey (Table _ _ v _) =
  renderKey <$> v

renderKey :: Key a -> Text
renderKey k =
  case k of
    StringKey v ->
      v
    IntKey v ->
      v

primaryKeyType :: Table -> D.ScalarAttributeType
primaryKeyType (Table _ p _ _) =
  keyType p

sortKeyType :: Table -> Maybe D.ScalarAttributeType
sortKeyType (Table _ _ s _) =
  keyType <$> s

keyType :: Key a -> D.ScalarAttributeType
keyType k =
  case k of
    StringKey _ ->
      D.S
    IntKey _ ->
      D.N

tableToCreate :: Table -> D.CreateTable
tableToCreate t =
  D.createTable
    (tableName t)
    (tableToSchemaElement t)
    (D.provisionedThroughput (readThroughput $ tableThroughput t) (writeThroughput $ tableThroughput t))
      & D.ctAttributeDefinitions .~ tableToAttributeDefintions t

tableToSchemaElement :: Table -> NonEmpty D.KeySchemaElement
tableToSchemaElement t =
  D.keySchemaElement (renderPrimaryKey t) D.Hash :| maybe [] (\x -> [D.keySchemaElement x D.Range]) (renderSortKey t)

tableToAttributeDefintions :: Table -> [D.AttributeDefinition]
tableToAttributeDefintions t = mconcat [
    [D.attributeDefinition (renderPrimaryKey t) (primaryKeyType t)]
  , maybe [] (\(x, y) -> [D.attributeDefinition x y]) ((,) <$> renderSortKey t <*> sortKeyType t)
  ]

toThroughput :: Throughput -> D.ProvisionedThroughput
toThroughput t =
  D.provisionedThroughput (readThroughput t) (writeThroughput t)
