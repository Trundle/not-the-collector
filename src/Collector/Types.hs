module Collector.Types (
  Output (..)
  ) where


data Output =
  GelfUdp String Int
  | Stdout deriving (Show)
