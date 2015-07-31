{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
module Main where

import           Control.Concurrent               (forkIO, threadDelay)
import           Control.Concurrent.Chan          (Chan, newChan, readChan)
import           Control.Monad                    (forM, forM_, forever, when)
import           Control.Monad.IO.Class           (MonadIO, liftIO)
import           Control.Monad.STM                (atomically)
import           Control.Monad.Trans.Class        (lift)
import           Control.Monad.Trans.Resource     (MonadResource, runResourceT)
import           Control.Monad.Trans.State.Strict (StateT, get, put)
import           Data.Aeson                       (Value, (.=))
import qualified Data.ByteString                  as BS
import           Data.Conduit                     (Conduit, Consumer, Producer,
                                                   Sink, ZipSink (..),
                                                   awaitForever, yield, ($$),
                                                   (=$=))
import qualified Data.Conduit.Binary              as CB
import qualified Data.Conduit.Lift                as CLift
import qualified Data.Conduit.List                as CL
import           Data.Conduit.TMChan              (TBMChan, newTBMChan,
                                                   sinkTBMChan, sourceTBMChan,
                                                   writeTBMChan)
import           Data.Foldable                    (sequenceA_)
import           Data.Function                    (on)
import qualified Data.List                        as L
import qualified Data.Map                         as M
import           Data.Maybe                       (fromJust)
import           Data.Ord                         (comparing)
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as TE
import           Data.Time.Clock.POSIX            (getPOSIXTime)
import           Network.BSD                      (getHostName)
import           Options.Applicative              (Parser, ParserInfo, command,
                                                   execParser, help, helper,
                                                   idm, info, metavar, progDesc,
                                                   short, strOption, subparser,
                                                   (<$>), (<**>), (<>))
import           Prelude                          hiding (FilePath)
import           System.Directory                 (makeAbsolute)
import           System.Exit                      (exitFailure)
import           System.FilePath                  (FilePath, takeDirectory)
import           System.FSNotify                  (Event (..), eventPath,
                                                   watchDirChan, withManager)
import qualified System.IO                        as IO

import qualified Collector.Config                 as Config
import           Collector.Types                  (Input (..), Output (..))
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


type Chunker = Monad m => Conduit BS.ByteString m BS.ByteString


lineChunker :: Chunker
lineChunker = CB.lines =$= CL.filter (not . BS.null)


filterModifiedPaths :: [FilePath] -> Event -> Bool
filterModifiedPaths paths (Modified path _) = path `L.elem` paths
filterModifiedPaths _ _ = False


chunkSize :: Int
chunkSize = 16 * 1024


toMessage :: MonadIO m =>
             T.Text ->
             FilePath ->
             Conduit BS.ByteString m Gelf.Message
toMessage hostName path = awaitForever $ \msg -> do
  timestamp <- liftIO getPOSIXTime
  yield Gelf.Message { Gelf.message = TE.decodeUtf8 msg
                     , Gelf.timestamp = timestamp
                     , Gelf.source = hostName
                     , Gelf.additionalFields = ["_source_file" .= path]
                     }


createOutput :: MonadResource m => Output -> Consumer Gelf.Message m ()
createOutput (GelfUdp host port) = Gelf.gelfUdpConsumer host port
createOutput (Stdout) = CL.mapM_ $ liftIO . print


createAndRunOutputs :: Value -> IO (M.Map String (TBMChan Gelf.Message))
createAndRunOutputs config = do
  outputConfigs <- case Config.parseOutputs config of
    Left e -> do
      putStrLn $ "Parsing of outputs failed: " ++ e
      exitFailure
    Right outputs -> return $ M.toList outputs
  when (null outputConfigs) $ do
    putStrLn "No outputs defined, nothing to do..."
    exitFailure
  outputs <- forM outputConfigs $ \(name, outputConfig) -> do
    chan <- atomically $ newTBMChan 512 -- XXX make size configurable
    _ <- forkIO . runResourceT $ sourceTBMChan chan $$ createOutput outputConfig
    return (name, chan)
  return $ M.fromList outputs


-- |Make paths of file inputs absolute.
toAbsoluteFile :: [Input] -> IO [Input]
toAbsoluteFile inputs = forM inputs $ \(File path outputs) -> do
  absolutePath <- makeAbsolute path
  return $ File absolutePath outputs


-- |Transform a list of inputs into a list of paths (i.e. all paths from file inputs).
collectPaths :: [Input] -> [FilePath]
collectPaths = map (\(File path _) -> path)


filesTail :: MonadIO m =>
             [FilePath] ->
             M.Map FilePath (TBMChan BS.ByteString) ->
             Consumer FilePath m ()
filesTail paths consumerChans = do
  handlesAndPositionsByPath <- liftIO $ forM paths $ \path -> do
    handle <- IO.openFile path IO.ReadMode
    IO.hSeek handle IO.SeekFromEnd 0
    pos <- IO.hTell handle
    return (path, (handle, pos))
  CLift.evalStateC
    (M.fromList handlesAndPositionsByPath)
    (contentsReader consumerChans)


contentsReader :: MonadIO m =>
                  M.Map FilePath (TBMChan BS.ByteString) ->
                  Consumer FilePath (StateT (M.Map String (IO.Handle, Integer)) m) ()
contentsReader consumerChans = awaitForever $ \path -> do
  handlesAndPositionsByPath <- lift get
  case M.lookup path handlesAndPositionsByPath of
    Nothing -> return ()
    Just (handle, pos) -> do
      (newPos, bytes) <- liftIO $ do
        IO.hSeek handle IO.AbsoluteSeek pos
        bytes <- BS.hGetNonBlocking handle chunkSize
        newPos <- IO.hTell handle
        return (newPos, bytes)
      lift $ put $ M.insert path (handle, newPos) handlesAndPositionsByPath
      case M.lookup path consumerChans of
        Just chan -> liftIO . atomically $ writeTBMChan chan bytes
        Nothing -> return ()


getOutputConsumer :: MonadIO m =>
                     M.Map String (TBMChan Gelf.Message) ->
                     [String] ->
                     Sink Gelf.Message m ()
getOutputConsumer outputs inputOutputs = (getZipSink . sequenceA_ . fmap ZipSink) sinks
  where
    getOutputChan name = fromJust $ M.lookup name outputs
    getOutputSink name = sinkTBMChan (getOutputChan name) False
    sinks = map getOutputSink inputOutputs


createAndRunChunkers :: T.Text ->
                        [Input] ->
                        M.Map String (TBMChan Gelf.Message) ->
                        IO (M.Map FilePath (TBMChan BS.ByteString))
createAndRunChunkers hostName inputs outputs = do
  consumerChansByPath <- forM inputs $ \(File path inputOutputs) -> do
    chan <- atomically $ newTBMChan 16
    _ <- forkIO . runResourceT $ sourceTBMChan chan
                               $$ lineChunker
                               =$= toMessage hostName path
                               =$= getOutputConsumer outputs inputOutputs
    return (path, chan)
  return $ M.fromList consumerChansByPath

createAndRunInputs :: T.Text ->
                      Value ->
                      M.Map String (TBMChan Gelf.Message) ->
                      IO ()
createAndRunInputs hostName config runningOutputs = do
  inputs <- case Config.parseInputs config of
    Left e -> do
      putStrLn $ "Parsing of inputs failed: " ++ e
      exitFailure
    Right inputs -> toAbsoluteFile inputs
  when (null inputs) $ do
    putStrLn "No inputs defined, nothing to do..."
    exitFailure
  consumerChans <- createAndRunChunkers hostName inputs runningOutputs
  let outputsByDirectory = (  L.groupBy ((==) `on` getDirectory)
                            . L.sortBy byDirectory) inputs
  withManager $ \manager -> forM_ outputsByDirectory $ \outputs -> do
      let directory = getDirectory $ head outputs
      let paths = collectPaths outputs
      chan <- liftIO newChan
      _ <- watchDirChan manager directory (filterModifiedPaths paths) chan
      _ <- forkIO $ runResourceT $ chanProducer chan
           $$  CL.map eventPath
           =$= filesTail paths consumerChans
      forever $ threadDelay maxBound
  where
    getDirectory (File path _) = takeDirectory path
    byDirectory = comparing getDirectory


run :: FilePath -> IO ()
run configPath = do
  json <- Config.parseConfig configPath
  outputs <- createAndRunOutputs json
  hostName <- getHostName
  createAndRunInputs (T.pack hostName) json outputs


main :: IO ()
main = execParser opts >>= \cmd -> case cmd of
  Run configPath -> run configPath
