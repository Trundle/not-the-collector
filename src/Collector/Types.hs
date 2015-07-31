module Collector.Types (
    Input (..)
  , Output (..)
  ) where

import           Prelude         hiding (FilePath)
import           System.FilePath (FilePath)


data Input = File FilePath [String]
           deriving (Show)


data Output =
  GelfUdp String Int
  | Stdout deriving (Show)
