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
          checkRead = (v ^. D.tdProvisionedThroughput >>= view D.ptdReadCapacityUnits) /= Just (readThroughput $ tableThroughput t)
          checkWrite = (v ^. D.tdProvisionedThroughput >>= view D.ptdWriteCapacityUnits) /= (Just . writeThroughput $ tableThroughput t)

        -- update modes
        when (checkRead || checkWrite) $ do
          liftIO . T.putStrLn . mconcat $ ["  ` updating throughput"]
          lift . void . A.send $ D.updateTable (renderTableName $ tableName t) &
            D.utProvisionedThroughput .~ Just (toThroughput $ tableThroughput t)

        -- failure modes
        when (v ^. D.tdKeySchema /= Just (tableToSchemaElement t)) $
          left . SchemaKeysMismatch $ tableName t

        when (v ^. D.tdAttributeDefinitions /= tableToAttributeDefintions t) $
          left . SchemaAttributeMismatch $ tableName t

    liftIO . T.putStrLn . mconcat $ ["  ` done"]

destroy :: Schema -> AWS ()
destroy schema =
  forM_ (schemaTables schema) $ \t -> do
    liftIO . T.putStrLn . mconcat $ ["Deleting table: ", renderTableName $ tableName t]
    void . A.send . D.deleteTable . renderTableName $ tableName t
    void . A.await D.tableNotExists . D.describeTable . renderTableName $ tableName t
    liftIO . T.putStrLn . mconcat $ ["  ` done"]
