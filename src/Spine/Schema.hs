{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
module Spine.Schema (
    Schema (..)
  , initialise
  , destroy
  ) where

import           Control.Lens (view, (^.), (.~), _Just)
import           Control.Monad.IO.Class (liftIO)

import           Numeric.Natural (Natural (..))

import           Data.Conduit ((=$=), ($$))
import qualified Data.Conduit.List as C
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Set as S


import           Mismi (AWS)
import qualified Mismi.Amazonka as A

import qualified Network.AWS.DynamoDB as D

import           P


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
    StringKey v ->
      D.S
    IntKey v ->
      D.N

data Throughput =
  Throughput {
      readThroughput :: Natural
    , writeThroughput :: Natural
    } deriving (Eq, Show)

kContext :: Key Text
kContext =
  StringKey "thing"

data Key a where
  IntKey :: Text -> Key Int
  StringKey :: Text -> Key Text

-- type Codec e v = Tuple (v -> String) (String -> Either e v)
--data Codec a where

{-
data Codec (Key a) where
  Encode :: a -> Codec a
  Decode :: a -> (Maybe (Codec a))
-}

tableToCreate :: Table -> D.CreateTable
tableToCreate t =
  D.createTable
    (tableName t)
    (D.keySchemaElement (renderPrimaryKey t) D.Hash :| maybe [] (\x -> [D.keySchemaElement x D.Range]) (renderSortKey t))
    (D.provisionedThroughput (readThroughput $ tableThroughput t) (writeThroughput $ tableThroughput t))
      & D.ctAttributeDefinitions .~ [
          D.attributeDefinition (renderPrimaryKey t) (primaryKeyType t)
        ] <> maybe [] (\(x, y) -> [D.attributeDefinition x y]) ((,) <$> renderSortKey t <*> sortKeyType t)

initialise :: Schema -> AWS ()
initialise schema = do
  tables <- A.paginate D.listTables =$=
    C.mapFoldable (view D.ltrsTableNames) $$
--    C.filter (T.isPrefixOf $ schemaPrefix schema) $$
    C.consume
  let
    indexed = S.fromList tables
    missing = filter (\t -> not $ S.member (tableName t) indexed) $ schemaTables schema

  forM_ missing $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Creating table: ", tableName t]
    void . A.send . tableToCreate $ t
    liftIO . T.putStrLn . mconcat $ ["  ` done"]

  -- update throughput


  -- ensure types are the same.

  forM_ (schemaTables schema) $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Waiting for table: ", tableName t]
    x <- A.await D.tableExists . D.describeTable $ tableName t

    case x of
      A.AcceptSuccess ->
        case x ^. D.drsTable of
          Nothing ->
            undefined
          Just v -> do
            -- failure modes
    --        (v ^. D.tdAttributeDefinitions)
    --        (v ^. D.tdKeySchema)

            let
              checkRead = v ^. D.tdProvisionedThroughput . _Just . D.ptdReadCapacityUnits . _Just /= (readThroughput $ tableThroughput t)
              checkWrite = v ^. D.tdProvisionedThroughput . _Just . D.ptdWriteCapacityUnits . _Just /= (writeThroughput $ tableThroughput t)

            -- update modes
            when (checkRead || checkWrite) $
    --        when (v ^. D.tdProvisionedThroughput /= Just (toThroughput $ tableThroughput t)) $
              void . A.send $ D.updateTable (tableName t) &
                D.utProvisionedThroughput .~ Just (toThroughput $ tableThroughput t)

      _ -> undefined
    liftIO . T.putStrLn . mconcat $ ["  ` done"]



toThroughput :: Throughput -> D.ProvisionedThroughput
toThroughput t =
  D.provisionedThroughput (readThroughput t) (writeThroughput t)

destroy :: Schema -> AWS ()
destroy schema =
  forM_ (schemaTables schema) $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Deleting table: ", tableName t]
    void . A.send . D.deleteTable $ tableName t
    void . A.await D.tableNotExists . D.describeTable $ tableName t
    liftIO . T.putStrLn . mconcat $ ["  ` done"]
