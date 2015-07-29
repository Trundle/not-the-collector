{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
module Main where

import           Control.Concurrent.Chan          (Chan, newChan, readChan)
import           Control.Monad                    (forever, liftM)
import           Control.Monad.IO.Class           (MonadIO, liftIO)
import           Control.Monad.Trans.Class        (lift)
import           Control.Monad.Trans.Resource     (runResourceT)
import           Control.Monad.Trans.State.Strict (StateT, get, put)
import           Data.Aeson                       (Value, toJSON, (.=))
import qualified Data.ByteString                  as BS
import           Data.Conduit                     (Conduit, Producer,
                                                   awaitForever, yield, ($$),
                                                   (=$=))
import qualified Data.Conduit.Binary              as CB
import qualified Data.Conduit.Lift                as CLift
import           Data.Conduit.List                as CL
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
import           Text.Toml                        (parseTomlDoc)

import qualified Graylog.Gelf                     as Gelf

data Command = Run String

run :: Parser Command
run = Run <$> strOption (  short 'f'
                        <> metavar "CONFIG"
                        <> help "Path to configuration file."
                        )

commands :: Parser Command
commands = subparser $ command "run" (info run (progDesc "Start the collector"))

opts :: ParserInfo Command
opts = info (commands <**> helper) idm


parseConfig :: FilePath -> IO Value
parseConfig path = do
  configContents <- liftM TE.decodeUtf8 $ BS.readFile path
  case parseTomlDoc path configContents of
    Left e -> print e >>= const exitFailure
    Right toml -> return $ toJSON toml


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


main :: IO ()
main = execParser opts >>= \cmd -> case cmd of
  Run configPath -> do
    json <- parseConfig configPath
    chan <- newChan
    withManager $ \manager -> do
      _ <- watchDirChan manager "." filterModifiedConfig chan
      runResourceT $ chanProducer chan
        $$  CL.map eventPath
        =$= fileTail "config.sample" lineChunker
        =$= Gelf.gelfUdpConsumer "127.0.0.1" 12201
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
