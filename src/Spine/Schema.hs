{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
module Spine.Schema (
    InitialisationError (..)
  , renderInitialisationError
  , initialise
  , destroy
  ) where

import           Control.Lens (view, (^.), (.~))
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Class (lift)

import           Data.Conduit ((=$=), ($$))
import qualified Data.Conduit.List as C
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.Text.IO as T
import qualified Data.Set as S


import           Mismi (AWS)
import qualified Mismi.Amazonka as A

import qualified Network.AWS.DynamoDB as D

import           P

import           Spine.Data

import           X.Control.Monad.Trans.Either (EitherT, left)

data InitialisationError =
    SchemaKeysMismatch TableName
  | SchemaAttributeMismatch TableName
  | InvariantMissingTable TableName
    deriving (Eq, Show)

renderInitialisationError :: InitialisationError -> Text
renderInitialisationError i =
  case i of
    SchemaKeysMismatch t ->
      mconcat ["The schema primary key names do not match for table [", renderTableName t, "]"]
    SchemaAttributeMismatch t ->
      mconcat ["The schema primary key types do not match for table [", renderTableName t, "]"]
    InvariantMissingTable t ->
      mconcat ["Spine invariant, the table [", renderTableName t, "] no longer exists."]

initialise :: Schema -> EitherT InitialisationError AWS ()
initialise schema = do
  tables <- lift $ A.paginate D.listTables =$=
    C.mapFoldable (view D.ltrsTableNames) $$
    C.consume
  let
    indexed = S.fromList tables
    missing = filter (\t -> not $ S.member (renderTableName $ tableName t) indexed) $ schemaTables schema

  forM_ missing $ \t -> lift $ do
    liftIO . T.putStrLn . mconcat $ ["Creating table: ", renderTableName $ tableName t]
    void . A.send . tableToCreate $ t
    liftIO . T.putStrLn . mconcat $ ["  ` done"]

  forM_ (schemaTables schema) $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Waiting for table: ", renderTableName $ tableName t]
    lift . void . A.await D.tableExists . D.describeTable . renderTableName $ tableName t
    x <- lift . A.send . D.describeTable . renderTableName $ tableName t
    case x ^. D.drsTable of
      Nothing ->
        left . InvariantMissingTable $ tableName t
      Just v -> do
        let
          throughput = tableThroughput t
          checkRead = checkThroughput (v ^. D.tdProvisionedThroughput >>= view D.ptdReadCapacityUnits) (readThroughput throughput)
          checkWrite = checkThroughput (v ^. D.tdProvisionedThroughput >>= view D.ptdWriteCapacityUnits) (writeThroughput throughput)

        -- update modes
        when ((not . isJustRight) checkRead || (not . isJustRight) checkWrite) $ do
          liftIO . T.putStrLn . mconcat $ ["  ` updating throughput"]
          lift . void . A.send $ D.updateTable (renderTableName $ tableName t) &
            D.utProvisionedThroughput .~ Just (toThroughput checkRead checkWrite)

        -- failure modes
        when (v ^. D.tdKeySchema /= Just (tableToSchemaElement t)) $
          left . SchemaKeysMismatch $ tableName t

        let
          sortit xs = sortOn (view D.adAttributeName) xs

        when ((sortit $ v ^. D.tdAttributeDefinitions) /= (sortit $ tableToAttributeDefintions t)) $
          left . SchemaAttributeMismatch $ tableName t

    liftIO . T.putStrLn . mconcat $ ["  ` done"]

destroy :: Schema -> AWS ()
destroy schema =
  forM_ (schemaTables schema) $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Deleting table: ", renderTableName $ tableName t]
    void . A.send . D.deleteTable . renderTableName $ tableName t
    void . A.await D.tableNotExists . D.describeTable . renderTableName $ tableName t
    liftIO . T.putStrLn . mconcat $ ["  ` done"]


renderPartitionKey :: Table -> Text
renderPartitionKey (Table _ p _ _) =
  renderItemKey p

renderSortKey :: Table -> Maybe Text
renderSortKey (Table _ _ s _) =
  renderItemKey <$> s

partitionKeyType :: Table -> D.ScalarAttributeType
partitionKeyType (Table _ p _ _) =
  itemKeyType p

sortKeyType :: Table -> Maybe D.ScalarAttributeType
sortKeyType (Table _ _ s _) =
  itemKeyType <$> s

itemKeyType :: ItemKey a -> D.ScalarAttributeType
itemKeyType a =
  case a of
    ItemIntKey _ ->
      D.N
    ItemStringKey _ ->
      D.S
    ItemBinaryKey _ ->
      D.B

tableToCreate :: Table -> D.CreateTable
tableToCreate t =
  D.createTable
    (renderTableName $ tableName t)
    (tableToSchemaElement t)
    (D.provisionedThroughput (minThroughput . readThroughput . tableThroughput $ t) (minThroughput . writeThroughput . tableThroughput $ t))
      & D.ctAttributeDefinitions .~ tableToAttributeDefintions t

tableToSchemaElement :: Table -> NonEmpty D.KeySchemaElement
tableToSchemaElement t =
  D.keySchemaElement (renderPartitionKey t) D.Hash :| maybe [] (\x -> [D.keySchemaElement x D.Range]) (renderSortKey t)

tableToAttributeDefintions :: Table -> [D.AttributeDefinition]
tableToAttributeDefintions t = mconcat [
    [D.attributeDefinition (renderPartitionKey t) (partitionKeyType t)]
  , maybe [] (\(x, y) -> [D.attributeDefinition x y]) ((,) <$> renderSortKey t <*> sortKeyType t)
  ]

toThroughput :: ThroughputPorridge -> ThroughputPorridge -> D.ProvisionedThroughput
toThroughput read write =
  D.provisionedThroughput (desiredThroughput read) (desiredThroughput write)
