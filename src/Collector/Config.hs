{-# LANGUAGE OverloadedStrings #-}
module Collector.Config (
    parseConfig
  , parseInputs
  , parseOutputs
  ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Monad       (liftM, mzero)
import           Data.Aeson          ((.:))
import qualified Data.Aeson          as A
import qualified Data.Aeson.Types    as AT
import qualified Data.ByteString     as BS
import qualified Data.Map.Strict     as M
import qualified Data.Text.Encoding  as TE
import           Prelude             hiding (FilePath)
import           System.Exit         (exitFailure)
import           System.FilePath     (FilePath)
import           Text.Toml           (parseTomlDoc)

import           Collector.Types     (Input (..), Output (..))


instance A.FromJSON Input where
  parseJSON (A.Object v) = do
    inputType <- v .: "type"
    case inputType of
      "file" -> File <$> v .: "path"
                     <*> v .: "outputs"
      _ -> fail $ "unknown input type: " ++ inputType
  parseJSON _ = mzero


instance A.FromJSON Output where
  parseJSON (A.Object v) = do
    outputType <- v .: "type" :: AT.Parser String
    case outputType of
      "gelf-udp" -> GelfUdp <$> v .: "host"
                            <*> v .: "port"
      "stdout" -> return Stdout
      _ -> fail $ "unknown output type: " ++ outputType
  parseJSON _ = mzero


parseConfig :: FilePath -> IO A.Value
parseConfig path = do
  configContents <- liftM TE.decodeUtf8 $ BS.readFile path
  case parseTomlDoc path configContents of
    Left e -> print e >>= const exitFailure
    Right toml -> return $ A.toJSON toml


parseOutputs :: A.Value -> Either String (M.Map String Output)
parseOutputs (A.Object config) = flip AT.parseEither config $ \obj ->
  obj .: "outputs"
parseOutputs _ = error "expected an object"


parseInputs :: A.Value -> Either String [Input]
parseInputs (A.Object config) = flip AT.parseEither config $ \obj -> do
   inputs <- obj .: "inputs" :: AT.Parser (M.Map String Input)
   return $ M.elems inputs
parseInputs _ = error "expected an object"
