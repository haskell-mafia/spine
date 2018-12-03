module Spine.P (
    void
  , forM
  , forM_
  , with
  , when
  , join
  , sortOn
  , find
  , mapMaybe
  , isJust
  , renderIntegral
  ) where

import           Control.Monad (forM, forM_, void, when, join)
import           Data.Foldable (find)
import           Data.List (sortBy)
import           Data.Maybe (mapMaybe, isJust)
import           Data.Ord (comparing)
import           Data.Text (Text)
import qualified Data.Text as T

with :: Functor f => f a -> (a -> b) -> f b
with xs f =
  fmap f xs
{-# INLINE with #-}

sortOn :: (Ord o) => (a -> o) -> [a] -> [a]
sortOn = sortBy . comparing
{-# INLINE sortOn #-}

renderIntegral :: (Show a, Integral a) => a -> Text
renderIntegral = T.pack . show
