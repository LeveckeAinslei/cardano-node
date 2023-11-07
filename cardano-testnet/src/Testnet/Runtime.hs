{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module Testnet.Runtime
  ( LeadershipSlot(..)
  , NodeLoggingFormat(..)
  , PaymentKeyInfo(..)
  , PaymentKeyPair(..)
  , StakingKeyPair(..)
  , TestnetRuntime(..)
  , NodeRuntime(..)
  , PoolNode(..)
  , PoolNodeKeys(..)
  , Delegator(..)
  , allNodes
  , poolSprockets
  , poolNodeStdout
  , readNodeLoggingFormat
  , startNode
  , ShelleyGenesis(..)
  , shelleyGenesis
  , getStartTime
  , fromNominalDiffTimeMicro
  ) where

import           Cardano.Api

import qualified Cardano.Chain.Genesis as G
import           Cardano.Crypto.ProtocolMagic (RequiresNetworkMagic (..))
import           Cardano.Ledger.Crypto (StandardCrypto)
import           Cardano.Ledger.Shelley.Genesis
import           Cardano.Node.Configuration.POM
import qualified Cardano.Node.Protocol.Byron as Byron
import           Cardano.Node.Types

import           Prelude

import qualified Control.Concurrent as IO
import           Control.Exception
import           Control.Monad
import qualified Control.Monad.Class.MonadTimer.SI as MT
import           Control.Monad.Error.Class
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Except.Extra
import           Control.Monad.Trans.Resource
import           Control.Tracer (nullTracer)
import qualified Data.Aeson as A
import qualified Data.List as List
import           Data.Maybe
import           Data.Text (Text)
import           Data.Time.Clock (UTCTime)
import           GHC.Generics (Generic)
import qualified GHC.IO.Handle as IO
import           GHC.Stack
import qualified GHC.Stack as GHC
import           Network.Mux.Bearer (MakeBearer (..), makeSocketBearer)
import qualified System.Directory as IO
import           System.FilePath
import qualified System.IO as IO
import qualified System.Process as IO

import qualified Hedgehog as H
import           Hedgehog.Extras.Stock.IO.Network.Sprocket (Sprocket (..))
import qualified Hedgehog.Extras.Stock.IO.Network.Sprocket as IO

import           Testnet.Filepath
import           Testnet.Process.Run
import           Testnet.Start.Types

import           Cardano.Network.Ping (NodeVersion (..))
import qualified Codec.CBOR.Decoding as CBOR
import qualified Codec.CBOR.Encoding as CBOR
import qualified Codec.CBOR.Read as CBOR
import qualified Codec.CBOR.Write as CBOR
import qualified Data.ByteString.Lazy as LBS
import qualified Data.List as L
import           Data.List.NonEmpty (NonEmpty)
import           Data.Word (Word32)
import           Network.Mux (MiniProtocolNum (..))
import           Network.Mux.Compat (MiniProtocolDir (..))
import qualified Network.Mux.Timeout as NM
import           Network.Mux.Types (MuxSDU, MuxSDUHeader (..), RemoteClockModel (..))
import qualified Network.Mux.Types as NM
import qualified Network.Socket as Socket

data TestnetRuntime = TestnetRuntime
  { configurationFile :: FilePath
  , shelleyGenesisFile :: FilePath
  , testnetMagic :: Int
  , poolNodes :: [PoolNode]
  , wallets :: [PaymentKeyInfo]
  , delegators :: [Delegator]
  }

data NodeRuntime = NodeRuntime
  { nodeName :: String
  , nodeSprocket :: Sprocket
  , nodeStdinHandle :: IO.Handle
  , nodeStdout :: FilePath
  , nodeStderr :: FilePath
  , nodeProcessHandle :: IO.ProcessHandle
  }

data PoolNode = PoolNode
  { poolRuntime :: NodeRuntime
  , poolKeys :: PoolNodeKeys
  }

data PoolNodeKeys = PoolNodeKeys
  { poolNodeKeysColdVkey :: FilePath
  , poolNodeKeysColdSkey :: FilePath
  , poolNodeKeysVrfVkey :: FilePath
  , poolNodeKeysVrfSkey :: FilePath
  , poolNodeKeysStakingVkey :: FilePath
  , poolNodeKeysStakingSkey :: FilePath
  } deriving (Eq, Show)

data PaymentKeyPair = PaymentKeyPair
  { paymentVKey :: FilePath
  , paymentSKey :: FilePath
  } deriving (Eq, Show)

data PaymentKeyInfo = PaymentKeyInfo
  { paymentKeyInfoPair :: PaymentKeyPair
  , paymentKeyInfoAddr :: Text
  } deriving (Eq, Show)

data StakingKeyPair = StakingKeyPair
  { stakingVKey :: FilePath
  , stakingSKey :: FilePath
  } deriving (Eq, Show)

data Delegator = Delegator
  { paymentKeyPair :: PaymentKeyPair
  , stakingKeyPair :: StakingKeyPair
  } deriving (Eq, Show)

data LeadershipSlot = LeadershipSlot
  { slotNumber  :: Int
  , slotTime    :: Text
  } deriving (Eq, Show, Generic, FromJSON)


poolNodeStdout :: PoolNode -> FilePath
poolNodeStdout = nodeStdout . poolRuntime

poolSprockets :: TestnetRuntime -> [Sprocket]
poolSprockets = fmap (nodeSprocket . poolRuntime) . poolNodes

shelleyGenesis :: (H.MonadTest m, MonadIO m, HasCallStack) => TestnetRuntime -> m (ShelleyGenesis StandardCrypto)
shelleyGenesis TestnetRuntime{shelleyGenesisFile} = withFrozenCallStack $
  H.evalEither =<< H.evalIO (A.eitherDecodeFileStrict' shelleyGenesisFile)

getStartTime
  :: (H.MonadTest m, MonadIO m, HasCallStack)
  => FilePath -> TestnetRuntime -> m UTCTime
getStartTime tempRootPath TestnetRuntime{configurationFile} = withFrozenCallStack $ H.evalEither <=< H.evalIO . runExceptT $ do
  byronGenesisFile <-
    decodeNodeConfiguration configurationFile >>= \case
      NodeProtocolConfigurationCardano NodeByronProtocolConfiguration{npcByronGenesisFile} _ _ _ _ ->
        pure $ unGenesisFile npcByronGenesisFile
  let byronGenesisFilePath = tempRootPath </> byronGenesisFile
  G.gdStartTime . G.configGenesisData <$> decodeGenesisFile byronGenesisFilePath
  where
    decodeNodeConfiguration :: FilePath -> ExceptT String IO NodeProtocolConfiguration
    decodeNodeConfiguration file = do
      partialNodeCfg <- ExceptT $ A.eitherDecodeFileStrict' file
      fmap ncProtocolConfig . liftEither . makeNodeConfiguration $ defaultPartialNodeConfiguration <> partialNodeCfg
    decodeGenesisFile :: FilePath -> ExceptT String IO G.Config
    decodeGenesisFile fp = withExceptT displayError $
      Byron.readGenesis (GenesisFile fp) Nothing RequiresNoMagic

readNodeLoggingFormat :: String -> Either String NodeLoggingFormat
readNodeLoggingFormat = \case
  "json" -> Right NodeLoggingFormatAsJson
  "text" -> Right NodeLoggingFormatAsText
  s -> Left $ "Unrecognised node logging format: " <> show s <> ".  Valid options: \"json\", \"text\""

allNodes :: TestnetRuntime -> [NodeRuntime]
allNodes tr = fmap poolRuntime (poolNodes tr)

data NodeStartFailure
  = ProcessRelatedFailure ProcessError
  | ExecutableRelatedFailure ExecutableError
  | FileRelatedFailure IOException
  | NodeExecutableError String
 -- | NodePortNotOpenError IOException
  | MaxSprocketLengthExceededError
  deriving Show

-- TODO: We probably want a check that this node has the necessary config files to run and
-- if it doesn't we fail hard.
-- | Start a node, creating file handles, sockets and temp-dirs.
startNode
  :: TmpAbsolutePath
  -- ^ The temporary absolute path
  -> String
  -- ^ The name of the node
  -> Int
  -- ^ Node port
  -> [String]
  -- ^ The command --socket-path will be added automatically.
  -> ExceptT NodeStartFailure (ResourceT IO) NodeRuntime
startNode tp node port nodeCmd = GHC.withFrozenCallStack $ do
  let tempBaseAbsPath = makeTmpBaseAbsPath tp
      socketDir = makeSocketDir tp
      logDir = makeLogDir tp

  liftIO $ createDirectoryIfMissingNew_ logDir
  void . liftIO $ createSubdirectoryIfMissingNew tempBaseAbsPath socketDir

  let nodeStdoutFile = logDir </> node <> ".stdout.log"
      nodeStderrFile = logDir </> node <> ".stderr.log"
      sprocket = Sprocket tempBaseAbsPath (socketDir </> node)

  hNodeStdout <- handleIOExceptT FileRelatedFailure $ IO.openFile nodeStdoutFile IO.WriteMode
  hNodeStderr <- handleIOExceptT FileRelatedFailure $ IO.openFile nodeStderrFile IO.ReadWriteMode


  unless (List.length (IO.sprocketArgumentName sprocket) <= IO.maxSprocketArgumentNameLength) $
     left MaxSprocketLengthExceededError

  let portString = show port

  nodeProcess
    <- firstExceptT ExecutableRelatedFailure
         $ hoistExceptT lift $ procNode $ mconcat
                       [ nodeCmd
                       , [ "--socket-path", IO.sprocketArgumentName sprocket
                         , "--port", portString
                         ]
                       ]

  (Just stdIn, _, _, hProcess, _)
    <- firstExceptT ProcessRelatedFailure $ initiateProcess
          $ nodeProcess
             { IO.std_in = IO.CreatePipe, IO.std_out = IO.UseHandle hNodeStdout
             , IO.std_err = IO.UseHandle hNodeStderr
             , IO.cwd = Just tempBaseAbsPath
             }

  -- We force the evaluation of initiateProcess so we can be sure that
  -- the process has started. This allows us to read stderr in order
  -- to fail early on errors generated from the cardano-node binary.
  mpid <- liftIO $ IO.getPid hProcess

  when (isNothing mpid)
    $ left $ NodeExecutableError $ mconcat ["startNode: ", node, "'s process did not start."]

  -- The process should have started so we wait a short amount of time to allow
  -- stderr to be populated with any errors from the cardano-node binary.
  liftIO $ IO.threadDelay 5_000_000

  stdErrContents <- liftIO $ IO.readFile nodeStderrFile

  if null stdErrContents
  then do

    -- TODO: Replace with cardano-ping library. However currently
    -- the library returns errors with printf.
    -- when (OS.os `List.elem` ["darwin", "linux"]) $ do
    --   void . handleIOExceptT NodePortNotOpenError
    --   $ IO.readProcess "lsof" ["-iTCP:" <> portString, "-sTCP:LISTEN", "-n", "-P"] ""

    let hints = Socket.defaultHints { Socket.addrSocketType = Socket.Stream }
        sduTimeout = 30 :: MT.DiffTime
        handshakeNum = MiniProtocolNum 0
    peer:_ <- liftIO $ Socket.getAddrInfo (Just hints) (Just "localhost") (Just (show port))
    liftIO $ bracket
      (Socket.socket (Socket.addrFamily peer) Socket.Stream Socket.defaultProtocol)
      Socket.close
      (\sd -> NM.withTimeoutSerial $ \timeoutfn -> do
        when (Socket.addrFamily peer /= Socket.AF_UNIX) $ do
          Socket.setSocketOption sd Socket.NoDelay 1
          Socket.setSockOpt sd Socket.Linger
            Socket.StructLinger
              { sl_onoff  = 1
              , sl_linger = 0
              }
          bearer <- getBearer makeSocketBearer sduTimeout nullTracer sd
          !t1_s <- NM.write bearer timeoutfn $ wrap handshakeNum InitiatorDir (handshakeReq versions pingOptsHandshakeQuery)
          (msg, !t1_e) <- nextMsg bearer timeoutfn handshakeNum

          pure ()
      )

    return $ NodeRuntime node sprocket stdIn nodeStdoutFile nodeStderrFile hProcess
  else left $ NodeExecutableError stdErrContents

wrap :: MiniProtocolNum -> MiniProtocolDir -> LBS.ByteString -> MuxSDU
wrap ptclNum ptclDir blob = NM.MuxSDU
  { msHeader = MuxSDUHeader
    { mhTimestamp = RemoteClockModel 0
    , mhNum       = ptclNum
    , mhDir       = ptclDir
    , mhLength    = fromIntegral $ LBS.length blob
    }
  , msBlob = blob
  }

handshakeReqEnc :: NonEmpty NodeVersion -> Bool -> CBOR.Encoding
handshakeReqEnc versions query =
      CBOR.encodeListLen 2
  <>  CBOR.encodeWord 0
  <>  CBOR.encodeMapLen (fromIntegral $ L.length versions)
  <>  mconcat [ encodeVersion (fixupVersion v)
              | v <- toList versions
              ]
  where
    -- Query is only available for NodeToNodeVersionV11 and higher, for smaller
    -- versions we send `InitiatorAndResponder`, in which case the remote side
    -- will do the handshake negotiation but it will reply with the right data.
    -- We shutdown the connection right after query, in most cases the remote
    -- side will not even have a chance to start using this connection as
    -- duplex (which could be possible if the node is using
    -- `NodeToNodeVersionV10`).
    fixupVersion :: NodeVersion -> NodeVersion
    fixupVersion v | not query = v
    fixupVersion (NodeToNodeVersionV4 a _)  = NodeToNodeVersionV4 a InitiatorAndResponder
    fixupVersion (NodeToNodeVersionV5 a _)  = NodeToNodeVersionV5 a InitiatorAndResponder
    fixupVersion (NodeToNodeVersionV6 a _)  = NodeToNodeVersionV6 a InitiatorAndResponder
    fixupVersion (NodeToNodeVersionV7 a _)  = NodeToNodeVersionV7 a InitiatorAndResponder
    fixupVersion (NodeToNodeVersionV8 a _)  = NodeToNodeVersionV8 a InitiatorAndResponder
    fixupVersion (NodeToNodeVersionV9 a _)  = NodeToNodeVersionV9 a InitiatorAndResponder
    fixupVersion (NodeToNodeVersionV10 a _) = NodeToNodeVersionV10 a InitiatorAndResponder
    fixupVersion v = v


    encodeVersion :: NodeVersion -> CBOR.Encoding

    -- node-to-client
    encodeVersion (NodeToClientVersionV9 magic) =
          CBOR.encodeWord (9 `setBit` nodeToClientVersionBit)
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToClientVersionV10 magic) =
          CBOR.encodeWord (10 `setBit` nodeToClientVersionBit)
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToClientVersionV11 magic) =
          CBOR.encodeWord (11 `setBit` nodeToClientVersionBit)
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToClientVersionV12 magic) =
          CBOR.encodeWord (12 `setBit` nodeToClientVersionBit)
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToClientVersionV13 magic) =
          CBOR.encodeWord (13 `setBit` nodeToClientVersionBit)
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToClientVersionV14 magic) =
          CBOR.encodeWord (14 `setBit` nodeToClientVersionBit)
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToClientVersionV15 magic) =
          CBOR.encodeWord (15 `setBit` nodeToClientVersionBit)
      <> nodeToClientDataWithQuery magic
    encodeVersion (NodeToClientVersionV16 magic) =
          CBOR.encodeWord (16 `setBit` nodeToClientVersionBit)
      <>  nodeToClientDataWithQuery magic

    -- node-to-node
    encodeVersion (NodeToNodeVersionV1 magic) =
          CBOR.encodeWord 1
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToNodeVersionV2 magic) =
          CBOR.encodeWord 2
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToNodeVersionV3 magic) =
          CBOR.encodeWord 3
      <>  CBOR.encodeInt (fromIntegral magic)
    encodeVersion (NodeToNodeVersionV4  magic mode) = encodeWithMode 4  magic mode
    encodeVersion (NodeToNodeVersionV5  magic mode) = encodeWithMode 5  magic mode
    encodeVersion (NodeToNodeVersionV6  magic mode) = encodeWithMode 6  magic mode
    encodeVersion (NodeToNodeVersionV7  magic mode) = encodeWithMode 7  magic mode
    encodeVersion (NodeToNodeVersionV8  magic mode) = encodeWithMode 8  magic mode
    encodeVersion (NodeToNodeVersionV9  magic mode) = encodeWithMode 9  magic mode
    encodeVersion (NodeToNodeVersionV10 magic mode) = encodeWithMode 10 magic mode
    encodeVersion (NodeToNodeVersionV11 magic mode) = encodeWithMode 11 magic mode
    encodeVersion (NodeToNodeVersionV12 magic mode) = encodeWithMode 12 magic mode

    nodeToClientDataWithQuery :: Word32 -> CBOR.Encoding
    nodeToClientDataWithQuery magic
      =  CBOR.encodeListLen 2
      <> CBOR.encodeInt (fromIntegral magic)
      <> CBOR.encodeBool query

    encodeWithMode :: Word -> Word32 -> InitiatorOnly -> CBOR.Encoding
    encodeWithMode vn magic mode
      | vn >= 12 =
          CBOR.encodeWord vn
       <> CBOR.encodeListLen 4
       <> CBOR.encodeInt (fromIntegral magic)
       <> CBOR.encodeBool (modeToBool mode)
       <> CBOR.encodeInt 0 -- NoPeerSharing
       <> CBOR.encodeBool query
      | vn >= 11 =
          CBOR.encodeWord vn
       <> CBOR.encodeListLen 4
       <> CBOR.encodeInt (fromIntegral magic)
       <> CBOR.encodeBool (modeToBool mode)
       <> CBOR.encodeInt 0 -- NoPeerSharing
       <> CBOR.encodeBool query
      | otherwise =
          CBOR.encodeWord vn
      <>  CBOR.encodeListLen 2
      <>  CBOR.encodeInt (fromIntegral magic)
      <>  CBOR.encodeBool (modeToBool mode)

handshakeReq :: [NodeVersion] -> Bool -> LBS.ByteString
handshakeReq []     _     = LBS.empty
handshakeReq (v:vs) query = CBOR.toLazyByteString $ handshakeReqEnc (v:|vs) query

createDirectoryIfMissingNew :: HasCallStack => FilePath -> IO FilePath
createDirectoryIfMissingNew directory = GHC.withFrozenCallStack $ do
  IO.createDirectoryIfMissing True directory
  pure directory

createDirectoryIfMissingNew_ :: HasCallStack => FilePath -> IO ()
createDirectoryIfMissingNew_ directory = GHC.withFrozenCallStack $
  void $ createDirectoryIfMissingNew directory


createSubdirectoryIfMissingNew :: ()
  => HasCallStack
  => FilePath
  -> FilePath
  -> IO FilePath
createSubdirectoryIfMissingNew parent subdirectory = GHC.withFrozenCallStack $ do
  IO.createDirectoryIfMissing True $ parent </> subdirectory
  pure subdirectory
