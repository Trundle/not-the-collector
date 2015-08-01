{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
module Main where

import           Control.Concurrent               (forkIO, threadDelay)
import           Control.Monad                    (forM, forM_, forever, when)
import           Control.Monad.IO.Class           (MonadIO, liftIO)
import           Control.Monad.STM                (atomically)
import           Control.Monad.Trans.Class        (lift)
import           Control.Monad.Trans.Resource     (MonadResource, runResourceT)
import           Control.Monad.Trans.State.Strict (StateT, get, put)
import           Data.Aeson                       ((.=))
import qualified Data.ByteString                  as BS
import           Data.Conduit                     (Conduit, Consumer, Sink,
                                                   ZipSink (..), awaitForever,
                                                   yield, ($$), (=$=))
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
                                                   watchDir, withManager)
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


createAndRunOutputs :: [(String, Output)] ->
                       IO (M.Map String (TBMChan Gelf.Message))
createAndRunOutputs outputConfigs = do
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


fileTail :: MonadIO m =>
            FilePath ->
            Conduit Event m BS.ByteString
fileTail path = do
  (handle, position) <- liftIO $ do
    handle <- IO.openFile path IO.ReadMode
    IO.hSeek handle IO.SeekFromEnd 0
    pos <- IO.hTell handle
    return (handle, pos)
  CLift.evalStateC position (contentsReader handle)


contentsReader :: MonadIO m =>
                  IO.Handle ->
                  Conduit Event (StateT Integer m) BS.ByteString
contentsReader handle = awaitForever $ \_ -> do
  pos <- lift get
  (newPos, bytes) <- liftIO $ do
    IO.hSeek handle IO.AbsoluteSeek pos
    bytes <- BS.hGetNonBlocking handle chunkSize
    newPos <- IO.hTell handle
    return (newPos, bytes)
  lift $ put newPos
  yield bytes


getOutputConsumer :: MonadIO m =>
                     M.Map String (TBMChan Gelf.Message) ->
                     [String] ->
                     Sink Gelf.Message m ()
getOutputConsumer outputs inputOutputs = (getZipSink . sequenceA_ . fmap ZipSink) sinks
  where
    getOutputChan name = fromJust $ M.lookup name outputs
    getOutputSink name = sinkTBMChan (getOutputChan name) False
    sinks = map getOutputSink inputOutputs


createAndRunFileInputs :: T.Text ->
                          [Input] ->
                          M.Map String (TBMChan Gelf.Message) ->
                          IO (M.Map FilePath (TBMChan Event))
createAndRunFileInputs hostName inputs outputs = do
  consumerChansByPath <- forM inputs $ \(File path inputOutputs) -> do
    chan <- atomically $ newTBMChan 16
    _ <- forkIO . runResourceT $ sourceTBMChan chan
                               $$  fileTail path
                               =$= lineChunker
                               =$= toMessage hostName path
                               =$= getOutputConsumer outputs inputOutputs
    return (path, chan)
  return $ M.fromList consumerChansByPath

createAndRunInputs :: T.Text ->
                      [Input] ->
                      M.Map String (TBMChan Gelf.Message) ->
                      IO ()
createAndRunInputs hostName inputs runningOutputs = do
  when (null inputs) $ do
    putStrLn "No inputs defined, nothing to do..."
    exitFailure
  absoluteInputs <- toAbsoluteFile inputs
  consumerChans <- createAndRunFileInputs hostName absoluteInputs runningOutputs
  let outputsByDirectory = (  L.groupBy ((==) `on` getDirectory)
                            . L.sortBy byDirectory) absoluteInputs
  withManager $ \manager -> forM_ outputsByDirectory $ \outputs -> do
      let directory = getDirectory $ head outputs
      let paths = collectPaths outputs
      _ <- watchDir manager directory (const True) (eventHandler consumerChans)
      forever $ threadDelay maxBound
  where
    getDirectory (File path _) = takeDirectory path
    byDirectory = comparing getDirectory
    eventHandler consumerChans event = do
      print event
      case M.lookup (eventPath event) consumerChans of
        Just chan -> atomically $ writeTBMChan chan event
        Nothing -> return ()


run :: FilePath -> IO ()
run configPath = do
  result <- Config.loadConfig configPath
  case result of
    Left e -> do
      putStrLn e
      exitFailure
    Right (inputs, outputs) -> do
      runningOutputs <- createAndRunOutputs outputs
      hostName <- getHostName
      createAndRunInputs (T.pack hostName) inputs runningOutputs


main :: IO ()
main = execParser opts >>= \cmd -> case cmd of
  Run configPath -> run configPath
