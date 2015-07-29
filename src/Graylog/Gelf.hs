{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
module Graylog.Gelf (
    Message (..)
  , encode
  , gelfUdpConsumer) where

import           Control.Monad.IO.Class       (liftIO)
import           Control.Monad.Trans.Resource (MonadResource)
import           Data.Aeson                   ((.=))
import qualified Data.Aeson                   as A
import           Data.Aeson.Types             (Pair)
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Lazy         as BSL
import           Data.Conduit                 (Consumer, bracketP, (=$=))
import qualified Data.Conduit.List            as CL
import qualified Data.Conduit.Network.UDP     as UDP
import           Data.Text                    (Text)
import           Data.Time.Clock.POSIX        (POSIXTime)
import qualified Network.Socket               as N


data Message =
  Message { message          :: Text
          , source           :: Text
          , timestamp        :: POSIXTime
          , additionalFields :: [Pair]
          } deriving (Show)

instance A.ToJSON Message where
  toJSON msg =
    A.object $ [ "version" .= A.String "1.1"
               , "host" .= source msg
               , "timestamp" .= (realToFrac (timestamp msg) :: Double)
               , "short_message" .= message msg
               ] ++ additionalFields msg

encode :: Message -> BSL.ByteString
encode = A.encode


chunkMagicBytes :: BSL.ByteString
chunkMagicBytes = BSL.pack [0x1e, 0x0f]


-- XXX always returns the first address - do something more clever
-- (e.g. IPv4 fallback etc)
resolve :: String -> Int -> IO N.AddrInfo
resolve hostname port = do
  let hints = N.defaultHints { N.addrFlags = [N.AI_ADDRCONFIG, N.AI_CANONNAME] }
  addrs <- N.getAddrInfo (Just hints) (Just hostname) (Just $ show port)
  return $ head addrs


connectAndConsume :: MonadResource m => N.AddrInfo -> Consumer BS.ByteString m ()
connectAndConsume addr = bracketP
  (N.socket (N.addrFamily addr) N.Datagram N.defaultProtocol)
  N.sClose
  (\sock -> do
       liftIO $ N.connect sock $ N.addrAddress addr
       UDP.sinkSocket sock)


gelfUdpConsumer :: MonadResource m => String -> Int -> Consumer Message m ()
gelfUdpConsumer hostname port = do
  addr <- liftIO $ resolve hostname port
  CL.map (BSL.toStrict . encode) =$= connectAndConsume addr
