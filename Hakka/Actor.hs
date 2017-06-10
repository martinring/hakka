module Hakka.Actor (
  ActorPath,  (//),
  ActorRef,
  ActorIO,
  Severity (Debug,Info,Warn,Error),
  (!),
  sender,
  self,
  parent,
  log,
  actor,
  become,
  noop,
  scheduleMessage,
  stop,
  actorSystem,
  terminate,
  liftIO
) where

import Prelude hiding (log)
import Data.List (partition)
import Control.Monad (forM_)
import Control.Concurrent.Chan
import Control.Concurrent (forkIO, threadDelay)
import Control.Exception
import Control.Monad.Trans.State.Lazy
import Control.Monad.IO.Class

--------------------------------------------------------------------------------
-- * Actor Paths and Actor Refs                                               --
--------------------------------------------------------------------------------

data ActorPath =
  RootActorPath { name :: String } |
  ChildActorPath { parentPath :: ActorPath, name :: String } deriving (Eq)

parentOrRoot (ChildActorPath p _) = p
parentOrRoot root = root

isChildOf :: ActorPath -> ActorPath -> Bool
isChildOf a@(ChildActorPath parent name) b = a == b || isChildOf parent b
isChildOf _ _ = False

instance Show ActorPath where
  show (RootActorPath uri) = uri
  show (ChildActorPath parent s) = (show parent) ++ "/" ++ s

isReserved :: Char -> Bool
isReserved = (flip elem) "[]?:#@!$()&%*+,;=/' "

validateSegment segment =
  if any isReserved segment
    then error $ "segment contains reserved characters: " ++ segment
    else segment

(//) :: ActorPath -> String -> ActorPath
(//) parent = ChildActorPath parent . validateSegment

newtype ActorRef m = ActorRef { actorPath :: ActorPath } deriving (Eq)

instance Show (ActorRef m) where
  show (ActorRef path) = "*" ++ show path

--------------------------------------------------------------------------------
-- * Signals, Messages and Envelopes                                          --
--------------------------------------------------------------------------------

data Signal = Stop | ChildFailed ActorPath String deriving Show

data Message m = Message m | Signal Signal deriving Show

data Envelope m = Envelope {
  from :: ActorRef m,
  to :: ActorRef m,
  message :: Message m
} deriving Show

data SystemMessage m =
  NewActor {
    path :: ActorPath,
    inbox :: Inbox m
  } |
  ActorTerminated ActorPath |
  Deliver (Envelope m) |
  Log ActorPath Severity String |
  Schedule Int (Envelope m)

data Severity = Debug | Info | Warn | Error deriving Show

--------------------------------------------------------------------------------
-- * Actors                                                                   --
--------------------------------------------------------------------------------

type ActorIO m n = StateT (ActorContext m) IO n

type Inbox m = Chan (Envelope m)

data ActorContext m = ActorContext {
  ctxBehavior :: Behavior m,
  ctxSender :: ActorRef m,
  ctxSelf :: ActorRef m,
  ctxSend :: Envelope m -> IO (),
  ctxSchedule :: Int -> Envelope m -> IO (),
  ctxActor :: String -> Behavior m -> IO (ActorRef m),
  ctxLogMessage :: Severity -> String -> IO ()
}

data Behavior m = Behavior {
  handleMessage :: m -> ActorIO m (),
  handleSignal :: Signal -> ActorIO m ()
} | Stopped

defaultBehavior :: Show m => Behavior m
defaultBehavior = Behavior {
  handleMessage = \m ->
    log Warn $ "unhandled message received",
  handleSignal = \sig -> case sig of
    Stop -> stop
    ChildFailed path e ->
      log Error $ "child " ++ show path ++ " failed: " ++ e
}

(!) :: ActorRef m -> m -> ActorIO m ()
ref ! msg = do
  ctx <- get
  liftIO $ ctxSend ctx $ Envelope (ctxSelf ctx) ref (Message msg)


become :: (m -> ActorIO m ()) -> ActorIO m ()
become msgHandler = modify (\ctx -> ctx { ctxBehavior = (ctxBehavior ctx) { handleMessage = msgHandler } })

stop :: ActorIO m ()
stop = modify (\ctx -> ctx { ctxBehavior = Stopped })

scheduleMessage :: Int -> ActorRef m -> m -> ActorIO m ()
scheduleMessage duration ref msg = do
  ctx <- get
  liftIO $ ctxSchedule ctx duration $ Envelope (ctxSelf ctx) ref (Message msg)

self :: ActorIO m (ActorRef m)
self = get >>= (return . ctxSelf)

parent :: ActorIO m (ActorRef m)
parent = do
  ActorRef (ChildActorPath parent _) <- self
  return $ ActorRef parent

noop :: ActorIO m ()
noop = return ()

sender :: ActorIO m (ActorRef m)
sender = get >>= (return . ctxSender)

actor :: Show m => String -> (m -> ActorIO m ()) -> ActorIO m (ActorRef m)
actor name b = do
  ctx <- get
  ref <- liftIO $ ctxActor ctx name (defaultBehavior { handleMessage = b })
  return ref

log :: Severity -> String -> ActorIO m ()
log s msg = do
  ctx <- get
  liftIO $ ctxLogMessage ctx s msg

handleMsg :: Behavior m -> Message m -> ActorIO m ()
handleMsg (Behavior h _) (Message msg) = h msg
handleMsg (Behavior _ h) (Signal sig) = h sig

runActor :: Show m => Chan (SystemMessage m) -> Behavior m -> IO (Inbox m)
runActor system b = do
  inbox <- newChan
  let loop Stopped = return ()
      loop bhv = do
        Envelope sender self payload <- readChan inbox
        let send = writeChan system . Deliver
        let schedule d = writeChan system . Schedule d
        let actor name b = do
              inbox <- runActor system b
              let path = (actorPath self) // name
              writeChan system $ NewActor path inbox
              return $ ActorRef path
        let log s msg = writeChan system $ Log (actorPath self) s msg
        let context = ActorContext b sender self send schedule actor log
        let handler e = do
              let path = actorPath self
              let signal = Signal $ ChildFailed path (displayException (e :: SomeException))
              send $ Envelope self (ActorRef (parentPath path)) signal
              return Stopped
        let calculateNewBehavior = do
              ctx <- execStateT (handleMsg bhv payload) context
              return $ ctxBehavior ctx
        newBehavior <- catch calculateNewBehavior handler
        loop newBehavior
  forkIO $ loop b
  return inbox

--------------------------------------------------------------------------------
-- Actor Systems                                                              --
--------------------------------------------------------------------------------

data ActorSystem = ActorSystem {
  terminate :: IO ()
}

actorSystem :: Show m => String -> ActorIO m a -> IO ActorSystem
actorSystem name init = do
  system <- newChan
  let rootRef = ActorRef (RootActorPath $ validateSegment name)
  let loop actors = do
        msg <- readChan system
        let deliver msg@(Envelope from to payload)
              | to == rootRef = case payload of
                  Signal Stop -> do
                    putStrLn "System Stopped"
                  Signal (ChildFailed p e) -> do
                    putStrLn $ "Actor Failed: " ++ show p ++ ": " ++ e
                    writeChan system $ ActorTerminated p
                  Message m -> do
                    putStrLn $ "plain message sent to root actor: " ++ show m
              | otherwise = case lookup (actorPath to) actors of
                  Just recipient -> do
                    writeChan recipient msg
                  Nothing -> do
                    putStrLn $ "message could not be delivered to " ++ (show (actorPath to))
        let calculateNewState = case msg of
              NewActor path inbox -> do
                return $ Just ((path,inbox):actors)
              ActorTerminated path -> do
                let (remove,retain) = partition ((`isChildOf` path) . fst) actors
                forM_ remove $ \(p,a) -> writeChan a $ Envelope rootRef (ActorRef p) (Signal Stop)
                return $ Just retain
              Schedule duration msg -> do
                forkIO $ do
                  threadDelay $ duration * 1000
                  deliver msg
                return $ Just actors
              Log path s msg -> do
                putStrLn $ "[" ++ show s ++ "] [" ++ show path ++ "]: " ++ msg
                return $ Just actors
              Deliver msg -> do
                deliver msg
                return $ Just actors
        let handler e = do
              putStrLn $ "Actor system '" ++ name ++  "' failed: " ++ (displayException (e :: SomeException))
              return Nothing
        newState <- catch calculateNewState handler
        maybe (return ()) loop newState
  forkIO $ loop []
  let send = writeChan system . Deliver
  let schedule d = writeChan system . Schedule d
  let actor name behavior = do
        inbox <- runActor system behavior
        let path = (actorPath rootRef) // name
        writeChan system $ NewActor path inbox
        return $ ActorRef path
  let log s msg = writeChan system $ Log (actorPath rootRef) s msg
  let rootContext = ActorContext Stopped rootRef rootRef send schedule actor log
  let stopMessage = Envelope rootRef rootRef $ Signal Stop
  let stop = writeChan system $ Deliver stopMessage
  evalStateT init rootContext
  return $ ActorSystem stop
