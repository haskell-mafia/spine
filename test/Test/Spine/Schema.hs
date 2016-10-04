{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Spine.Schema (
    withClean
  ) where

import           Spine.Data
import           Spine.Schema

import           Mismi (AWS, awsBracket)

import           P

import           X.Control.Monad.Trans.Either (eitherT)

withClean :: Schema -> AWS () -> AWS a -> AWS a
withClean e clean f =
  awsBracket (eitherT (fail . show) pure $ initialise e) (const $ clean) (const $ clean >> f)
