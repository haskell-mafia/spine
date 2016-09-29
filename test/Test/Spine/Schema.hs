{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Spine.Schema (
    withClean
  ) where

import           Spine.Schema

import           Mismi (AWS, awsBracket)

import           P

withClean :: Schema -> AWS () -> AWS a -> AWS a
withClean e clean f =
  awsBracket (initialise e) (const $ clean) (const $ clean >> f)
