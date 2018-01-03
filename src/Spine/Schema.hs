{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
module Spine.Schema (
    InitialisationError (..)
  , renderInitialisationError
  , initialise
  , destroy
  ) where

import           Control.Lens (view, (^.), (.~), _Just)
import           Control.Lens (Fold, (^?), folding, concatOf)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Class (lift)

import           Data.Conduit ((=$=), ($$))
import qualified Data.Conduit.List as C
import qualified Data.Align as Align
import qualified Data.These as These
import qualified Data.Map.Strict as Map

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
          indexes = v ^. D.tdGlobalSecondaryIndexes


        -- failure modes
        when (v ^. D.tdKeySchema /= Just (tableToSchemaElement t)) $
          left . SchemaKeysMismatch $ tableName t

        -- update modes
        when ((not . isJustRight) checkRead || (not . isJustRight) checkWrite) $ do
          liftIO . T.putStrLn . mconcat $ ["  ` updating throughput"]
          lift . void . A.send $ D.updateTable (renderTableName $ tableName t) &
            D.utProvisionedThroughput .~ Just (toThroughput checkRead checkWrite)

        -- Secondary Indexes
        lift $ updateGlobalSecondayIndexes t indexes

    x2 <- lift . A.send . D.describeTable . renderTableName $ tableName t
    case x2 ^. D.drsTable of
      Nothing ->
        left . InvariantMissingTable $ tableName t
      Just v -> do
        let
          attrs = v ^. D.tdAttributeDefinitions
          sortit xs = sortOn (view D.adAttributeName) xs
        when ((sortit $ attrs) /= (sortit $ tableToAttributeDefintions t)) $
          left . SchemaAttributeMismatch $ tableName t


    liftIO . T.putStrLn . mconcat $ ["  ` done"]

destroy :: Schema -> AWS ()
destroy schema =
  forM_ (schemaTables schema) $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Deleting table: ", renderTableName $ tableName t]
    void . A.send . D.deleteTable . renderTableName $ tableName t
    void . A.await D.tableNotExists . D.describeTable . renderTableName $ tableName t
    liftIO . T.putStrLn . mconcat $ ["  ` done"]

indexExists :: A.Wait D.UpdateTable
indexExists =
  A.Wait {
    A._waitName = "IndexExists"
  , A._waitAttempts = 25
  , A._waitDelay = 20
  , A._waitAcceptors = [
        A.matchError "ResourceInUse" A.AcceptRetry
      , A.matchAll D.ISActive A.AcceptSuccess indexesFold
      , matchMessage "Attempting to create an index which already exists" A.AcceptSuccess
      ]
  }

matchMessage :: Text -> A.Accept -> A.Acceptor a
matchMessage c a _ x =
  case x of
    Left e | Just (Just (A.ErrorMessage c)) == e ^? A._ServiceError . A.serviceMessage ->
      Just a
    _ ->
      Nothing

indexNotExists :: A.Wait D.UpdateTable
indexNotExists =
  A.Wait {
    A._waitName = "IndexNotExists"
  , A._waitAttempts = 25
  , A._waitDelay = 20
  , A._waitAcceptors =
      [A.matchError "ResourceNotFoundException" A.AcceptSuccess]
  }

indexesFold :: Fold D.UpdateTableResponse D.IndexStatus
indexesFold =
  D.utrsTableDescription . _Just . folding (concatOf D.tdGlobalSecondaryIndexes) . D.gsidIndexStatus . _Just

renderPartitionKey :: Table -> Text
renderPartitionKey (Table _ p _ _ _) =
  renderItemKey p

renderSortKey :: Table -> Maybe Text
renderSortKey (Table _ _ s _ _) =
  renderItemKey <$> s

partitionKeyType :: Table -> D.ScalarAttributeType
partitionKeyType (Table _ p _ _ _) =
  itemKeyType p

sortKeyType :: Table -> Maybe D.ScalarAttributeType
sortKeyType (Table _ _ s _ _) =
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
  let
    create = D.createTable
      (renderTableName $ tableName t)
      (tableToSchemaElement t)
      (D.provisionedThroughput (minThroughput . readThroughput . tableThroughput $ t) (minThroughput . writeThroughput . tableThroughput $ t))
        & D.ctAttributeDefinitions .~ tableToAttributeDefintions t
  in
    case tableToGlobalSecondaryIndexes t of
      [] ->
        create
      xs ->
        create & D.ctGlobalSecondaryIndexes .~ xs


keySchemaElement :: ItemKey a -> Maybe (ItemKey b) -> NonEmpty D.KeySchemaElement
keySchemaElement a b =
  D.keySchemaElement (renderItemKey a) D.Hash :| maybe [] (\x -> [D.keySchemaElement (renderItemKey x) D.Range]) b



tableToCreateGlobalSecondaryIndexAction :: SecondaryIndex -> D.CreateGlobalSecondaryIndexAction
tableToCreateGlobalSecondaryIndexAction (SecondaryIndex i k s p th) =
  D.createGlobalSecondaryIndexAction
    (renderIndexName i)
    (keySchemaElement k s)
    (maybe (D.projection & D.pProjectionType .~ Just D.KeysOnly) toProjection p)
    (D.provisionedThroughput (minThroughput . readThroughput $ th) (minThroughput . writeThroughput $ th))

tableToGlobalSecondaryIndexes :: Table -> [D.GlobalSecondaryIndex]
tableToGlobalSecondaryIndexes t =
  with (tableGlobalSecondaryIndexes t) $ \(SecondaryIndex i k s p th) ->
    D.globalSecondaryIndex
      (renderIndexName i)
      (keySchemaElement k s)
      (maybe (D.projection & D.pProjectionType .~ Just D.KeysOnly) toProjection p)
      (D.provisionedThroughput (minThroughput . readThroughput $ th) (minThroughput . writeThroughput $ th))

toProjection :: Projection -> D.Projection
toProjection (Projection attributes) =
  D.projection
    & D.pProjectionType .~ Just D.Include
    & D.pNonKeyAttributes .~ case attributes of
        h : t ->
          Just $ renderKey h :| fmap renderKey t
        [] ->
          Nothing

tableToSchemaElement :: Table -> NonEmpty D.KeySchemaElement
tableToSchemaElement (Table _ p s _ _) =
  keySchemaElement p s

tableToAttributeDefintions :: Table -> [D.AttributeDefinition]
tableToAttributeDefintions t = mconcat [
    [D.attributeDefinition (renderPartitionKey t) (partitionKeyType t)]
  , maybe [] (\(x, y) -> [D.attributeDefinition x y]) ((,) <$> renderSortKey t <*> sortKeyType t)
  , join . with (tableGlobalSecondaryIndexes t) $ \(SecondaryIndex _ p s _ _) -> mconcat [
       [D.attributeDefinition (renderItemKey p) (itemKeyType p)]
     , maybe [] (\(x, y) -> [D.attributeDefinition x y]) ((,) <$> fmap renderItemKey s <*> fmap itemKeyType s)
     ]
  ]

toThroughput :: ThroughputPorridge -> ThroughputPorridge -> D.ProvisionedThroughput
toThroughput read write =
  D.provisionedThroughput (desiredThroughput read) (desiredThroughput write)


updateGlobalSecondayIndexes :: Table -> [D.GlobalSecondaryIndexDescription] -> AWS ()
updateGlobalSecondayIndexes t indexes = do
  let
    expected = Map.fromList $ (\i -> (renderIndexName $ indexName i, i)) <$> tableGlobalSecondaryIndexes t
    current = Map.fromList . flip mapMaybe indexes $ \i ->
      with (i ^. D.gsidIndexName) $ \z ->
        (z, (z, i))
    these = Map.elems $ Align.align current expected

  -- Delete
  forM_ (These.catThis these) $ \(name, x) -> do
    liftIO . T.putStrLn . mconcat $ ["  ` deleting global secondary index: ", name]
    let
      delete =
        void . A.await indexNotExists $ D.updateTable (renderTableName $ tableName t)
          & D.utGlobalSecondaryIndexUpdates .~ [
              D.globalSecondaryIndexUpdate & D.gsiuDelete .~ Just (D.deleteGlobalSecondaryIndexAction name)
            ]

    case x ^. D.gsidIndexStatus of
      Nothing ->
        delete
      Just D.ISActive ->
        delete
      Just D.ISCreating ->
        delete
      Just D.ISDeleting ->
        pure ()
      Just D.ISUpdating ->
        delete
    liftIO . T.putStrLn . mconcat $ ["    ` done"]

  -- Create
  forM_ (These.catThat these) $ \s -> do
    liftIO . T.putStrLn . mconcat $ ["  ` creating global secondary: ", renderIndexName $ indexName s]
    void . A.await indexExists $ D.updateTable (renderTableName $ tableName t)
      & D.utAttributeDefinitions .~ tableToAttributeDefintions t
      & D.utGlobalSecondaryIndexUpdates .~ [
          D.globalSecondaryIndexUpdate & D.gsiuCreate .~ Just (tableToCreateGlobalSecondaryIndexAction s)
        ]
    liftIO . T.putStrLn . mconcat $ ["    ` done"]

  -- Update
  forM_ (These.catThese these) $ \((name, cth), (SecondaryIndex _ _ _ _ th)) -> do
    let
      checkIndexRead =
        checkThroughput (cth ^. D.gsidProvisionedThroughput >>= view D.ptdReadCapacityUnits) (readThroughput th)
      checkIndexWrite =
        checkThroughput (cth ^. D.gsidProvisionedThroughput >>= view D.ptdWriteCapacityUnits) (writeThroughput th)

    when ((not . isJustRight) checkIndexRead || (not . isJustRight) checkIndexWrite) $ do
      liftIO . T.putStrLn . mconcat $ ["  ` updating global secondary index throughput"]
      void . A.send $ D.updateTable (renderTableName $ tableName t)
        & D.utAttributeDefinitions .~ tableToAttributeDefintions t
        & D.utGlobalSecondaryIndexUpdates .~ [
            D.globalSecondaryIndexUpdate & D.gsiuUpdate .~ Just (
               D.updateGlobalSecondaryIndexAction
                 name
                 (D.provisionedThroughput (minThroughput . readThroughput $ th) (minThroughput . writeThroughput $ th)))
          ]
      liftIO . T.putStrLn . mconcat $ ["    ` done"]
