
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
module Main where

import           Control.Concurrent               (forkIO)
import           Control.Concurrent.Chan          (Chan, newChan, readChan)
import           Control.Monad                    (forM, forever)
import           Control.Monad.IO.Class           (MonadIO, liftIO)
import           Control.Monad.STM                (atomically)
import           Control.Monad.Trans.Class        (lift)
import           Control.Monad.Trans.Resource     (MonadResource, runResourceT)
import           Control.Monad.Trans.State.Strict (StateT, get, put)
import           Data.Aeson                       (Value, (.=))
import qualified Data.ByteString                  as BS
import           Data.Conduit                     (Conduit, Consumer, Producer,
                                                   awaitForever, yield, ($$),
                                                   (=$=))
import qualified Data.Conduit.Binary              as CB
import qualified Data.Conduit.Lift                as CLift
import           Data.Conduit.List                as CL
import           Data.Conduit.TMChan              (TBMChan, newTBMChan,
                                                   sinkTBMChan, sourceTBMChan)
import qualified Data.Map                         as M
import           Data.Maybe                       (fromJust)
import           Data.Text.Encoding               as TE
import           Data.Time.Clock.POSIX            (getPOSIXTime)
import           Options.Applicative              (Parser, ParserInfo, command,
                                                   execParser, help, helper,
                                                   idm, info, metavar, progDesc,
                                                   short, strOption, subparser,
                                                   (<$>), (<**>), (<>))
import           Prelude                          hiding (FilePath)
import           System.Directory                 (makeAbsolute)
import           System.Exit                      (exitFailure)
import           System.FilePath                  (FilePath, takeFileName)
import           System.FSNotify                  (Event (..), eventPath,
                                                   watchDirChan, withManager)
import qualified System.IO                        as IO

import           Collector.Config                 (parseConfig, parseOutputs)
import           Collector.Types                  (Output (..))
import qualified Graylog.Gelf                     as Gelf

data Command = Run String

runCommand :: Parser Command
runCommand = Run <$> strOption (  short 'f'
                        <> metavar "CONFIG"
                        <> help "Path to configuration file."
                        )

commands :: Parser Command
commands = subparser $
  command "run" (info runCommand (progDesc "Start the collector"))

opts :: ParserInfo Command
opts = info (commands <**> helper) idm



chanProducer :: MonadIO m => Chan a -> Producer m a
chanProducer chan = forever $ yield =<< liftIO (readChan chan)


lineChunker :: Monad m => Conduit BS.ByteString m BS.ByteString
lineChunker = CB.lines


filterModifiedConfig :: Event -> Bool
filterModifiedConfig (Modified path _) = takeFileName path == "config.sample"
filterModifiedConfig _ = False


chunkSize :: Int
chunkSize = 16 * 1024


toMessage :: MonadIO m => FilePath -> Conduit BS.ByteString m Gelf.Message
toMessage path = awaitForever $ \msg -> do
  timestamp <- liftIO getPOSIXTime
  yield Gelf.Message { Gelf.message = TE.decodeUtf8 msg
                     , Gelf.timestamp = timestamp
                     , Gelf.source = "spam"
                     , Gelf.additionalFields = ["_source_file" .= path]
                     }


createOutput :: MonadResource m => Output -> Consumer Gelf.Message m ()
createOutput (GelfUdp host port) = Gelf.gelfUdpConsumer host port
createOutput (Stdout) = CL.mapM_ $ liftIO . print


createAndRunOutputs :: Value -> IO (M.Map String (TBMChan Gelf.Message))
createAndRunOutputs config = do
  outputConfigs <- case parseOutputs config of
    Left e -> do
      putStrLn $ "Parsing of outputs failed: " ++ e
      exitFailure
    Right outputs -> return outputs
  outputs <- forM (M.toList outputConfigs) $ \(name, outputConfig) -> do
    chan <- atomically $ newTBMChan 512 -- XXX make size configurable
    _ <- forkIO . runResourceT $ sourceTBMChan chan $$ createOutput outputConfig
    return (name, chan)
  return $ M.fromList outputs


run :: FilePath -> IO ()
run configPath = do
  json <- parseConfig configPath
  outputs <- createAndRunOutputs json

  chan <- newChan
  withManager $ \manager -> do
    _ <- watchDirChan manager "." filterModifiedConfig chan
    runResourceT $ chanProducer chan
      $$  CL.map eventPath
      =$= fileTail "config.sample" lineChunker
      =$= sinkTBMChan (fromJust $ M.lookup "stdout" outputs) True
  return ()
  where
    fileTail :: MonadIO m =>
                FilePath ->
                Conduit BS.ByteString m BS.ByteString ->
                Conduit FilePath m Gelf.Message
    fileTail path chunker = do
      (absolutePath, fileHandle, pos) <- liftIO $ do
         fileHandle <- IO.openFile "config.sample" IO.ReadMode
         absolutePath <- makeAbsolute path
         IO.hSeek fileHandle IO.SeekFromEnd 0
         pos <- IO.hTell fileHandle
         return (absolutePath, fileHandle, pos)
      CLift.evalStateC pos (content fileHandle)
        =$= chunker
        =$= toMessage absolutePath

    content :: MonadIO m => IO.Handle -> Conduit FilePath (StateT Integer m) BS.ByteString
    content handle = awaitForever $ \path -> do
      pos <- lift get
      (nextPos, bytes) <- liftIO $ do
        IO.hSeek handle IO.AbsoluteSeek pos
        bytes <- BS.hGetNonBlocking handle chunkSize
        nextPos <- IO.hTell handle
        return (nextPos, bytes)
      lift $ put nextPos
      yield bytes


main :: IO ()
main = execParser opts >>= \cmd -> case cmd of
  Run configPath -> run configPath
