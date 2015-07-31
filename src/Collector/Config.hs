-- This module deliberately declares orphan instances:
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
module Collector.Config (
    loadConfig
  , parseConfig
  ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Monad       (forM_, liftM, mzero)
import           Data.Aeson          ((.:))
import qualified Data.Aeson          as A
import qualified Data.Aeson.Types    as AT
import qualified Data.ByteString     as BS
import           Data.List           (intercalate)
import qualified Data.Map.Strict     as M
import qualified Data.Text           as T
import qualified Data.Text.Encoding  as TE
import           Prelude             hiding (FilePath)
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



parseConfig :: FilePath ->
               T.Text ->
               Either String ([Input], [(String, Output)])
parseConfig path contents = do
  toml <- case parseTomlDoc path contents of
    Left e -> Left $ "Error while loading config: " ++ show e
    Right t -> Right t
  let json = A.toJSON toml
  inputs <- parseInputs json
  outputs <- parseOutputs json
  checkConnections inputs outputs
  return (M.elems inputs, M.toList outputs)


loadConfig :: FilePath ->
              IO (Either String ([Input], [(String, Output)]))
loadConfig path = do
  configContents <- liftM TE.decodeUtf8 $ BS.readFile path
  return $ parseConfig path configContents


parseOutputs :: A.Value -> Either String (M.Map String Output)
parseOutputs (A.Object config) = flip AT.parseEither config $ \obj ->
  obj .: "outputs"
parseOutputs _ = error "expected an object"


parseInputs :: A.Value -> Either String (M.Map String Input)
parseInputs (A.Object config) = flip AT.parseEither config $ \obj ->
   obj .: "inputs" :: AT.Parser (M.Map String Input)
parseInputs _ = error "expected an object"


-- |Verify that every referenced output exists.
checkConnections :: M.Map String Input ->
                    M.Map String Output ->
                    Either String ()
checkConnections inputs outputs =
  forM_ (M.toList inputs) $ \(inputName, File _ inputOutputs) -> do
    let missingOutputs = filter outputNotExists inputOutputs
    if null missingOutputs then Right ()
      else Left $ "The following outputs of input "
           ++ inputName
           ++ " do not exist: "
           ++ intercalate ", " missingOutputs
  where
    outputNotExists = not . flip elem (M.keys outputs)
