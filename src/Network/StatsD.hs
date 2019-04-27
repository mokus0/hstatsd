{-# LANGUAGE RecordWildCards #-}
module Network.StatsD
    ( StatsD
    , mkStatsD, openStatsD, closeStatsD
    
    , Stat(..), stat
    , push, showStat
    ) where

import Control.Monad.Writer
import qualified Data.ByteString as BS
import Data.List
import qualified Data.Text.Lazy as T
import qualified Data.Text.Lazy.Encoding as T
import Network.Socket
import qualified Network.Socket.ByteString as BS
import qualified Network.Socket.ByteString.Lazy as BL

data StatsD = StatsD
    { connection    :: !Socket
    , prefix        :: !T.Text
    } deriving (Eq, Show)

mkStatsD :: Socket -> [String] -> StatsD
mkStatsD s prefix = StatsD
    { connection    = s
    , prefix        = T.pack . concat . concat $ [[part, "."] | part <- prefix]
    }

openStatsD host port prefix = do
    let hints = defaultHints
            { addrFamily = AF_INET
            , addrSocketType = Datagram
            }
    
    s <- socket AF_INET Datagram defaultProtocol
    
    addrInfos <- getAddrInfo (Just hints) (Just host) (Just port)
    case addrInfos of
        [] -> fail "Could not resolve host and/or port"
        addrInfo : _ -> connect s (addrAddress addrInfo)
    
    return (mkStatsD s prefix)

data Stat = Stat
    { bucket    :: !T.Text
    , val       :: !T.Text
    , unit      :: !T.Text
    , sample    :: !(Maybe Double)
    } deriving (Eq, Show)

stat :: (Num a, Show a) => [String] -> a -> String -> Maybe Double -> Stat
stat b v u = Stat (T.pack (intercalate "." b)) (T.pack (show v)) (T.pack u)

showStat = T.unpack . fmt T.empty

fmt prefix Stat{..} = T.concat $ execWriter $ do
    let colon = T.singleton ':'
        bar = T.singleton '|'
        bar_at = T.pack "|@"
    
    tell [prefix, bucket, colon, val, bar, unit]
    case sample of
        Nothing -> return ()
        Just sample -> tell [bar_at, T.pack (show sample)]

segment = id -- TODO: chunk things to fit within MTU

fmtMany prefix = map (T.encodeUtf8 . fmt prefix)

push statsd = mapM_ (BL.sendAll (connection statsd)) . segment . fmtMany (prefix statsd)

closeStatsD = close . connection
