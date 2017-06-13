{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-}

module Hakka.Actor (
  ActorRef,

  ActorIO,
  noop,

  actor,

  (!),
  schedule,
  scheduleOnce,

  sender,
  self,
  parent,

  log,
  Severity (Debug,Info,Warn,Error),
  
  become,
  stop,

  actorSystem,
  ActorSystem, terminate,

  Cancellable, cancel,

  liftIO
) where

import Prelude hiding (log)
import Data.List (partition)
import Data.Maybe (isNothing)
import Control.Monad (forM_,when)
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Concurrent (forkIO, threadDelay)
import Control.Exception
import Control.Monad.Trans.State.Strict
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

-- | Serializable handle to an Actor
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
  Schedule Int Int (MVar ()) (Envelope m)

--------------------------------------------------------------------------------
-- * Actors                                                                   --
--------------------------------------------------------------------------------

{- | 
  Actions which actors execute in response to a received message run within the
  ActorIO monad.
-}
newtype ActorIO m a = ActorIO (StateT (ActorContext m) IO a) 
  deriving (Monad,MonadIO,Applicative,Functor)

type Inbox m = Chan (Envelope m)

data ActorContext m = ActorContext {
  ctxBehavior :: Behavior m,
  ctxSender :: ActorRef m,
  ctxSelf :: ActorRef m,
  ctxSysMessage :: SystemMessage m -> IO (),
  ctxActor :: String -> Behavior m -> IO (ActorRef m)
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

-- | Send a message to an actor. Does not block (immediately returns control flow).
(!) :: ActorRef m -- ^ The reference to the receiving actor
    -> m -- ^ The message
    -> ActorIO m ()
ref ! msg = ActorIO $ do
  ctx <- get
  liftIO $ ctxSysMessage ctx $ Deliver $ Envelope (ctxSelf ctx) ref (Message msg)

-- | Switch the behavior of the current actor
become :: (m -> ActorIO m ()) -- ^ The new behavior
       -> ActorIO m ()
become msgHandler = ActorIO $ modify (\ctx -> ctx { ctxBehavior = (ctxBehavior ctx) { handleMessage = msgHandler } })

-- | Stop this actor
stop :: ActorIO m ()
stop = ActorIO $ modify (\ctx -> ctx { ctxBehavior = Stopped })

data Cancellable = forall m . Cancellable { cancel :: ActorIO m () }

-- | Schedule the delivery of a message in the future
schedule :: Int -- ^ Delay in milliseconds before the first message gets sent.
         -> Int -- ^ Delay in milliseconds between two subsequent messages
         -> ActorRef m -- ^ The reference to the receiving actor (Dereferenced in the future)
         -> m -- ^ The message
         -> ActorIO m Cancellable
schedule initialDuration duration ref msg = ActorIO $ do
  ctx <- get
  stop <- liftIO $ newEmptyMVar
  liftIO $ ctxSysMessage ctx $ Schedule initialDuration duration stop $ Envelope (ctxSelf ctx) ref (Message msg)
  return $ Cancellable $ liftIO $ tryPutMVar stop () >> return ()

-- | Schedule the delivery of a message in the future
scheduleOnce :: Int -- ^ Delay in milliseconds before the message gets sent.
                -> ActorRef m -- ^ The reference to the receiving actor (Dereferenced in the future)
                -> m -- ^ The message
                -> ActorIO m Cancellable
scheduleOnce duration ref msg = schedule duration (-1) ref msg

-- | Obtain the reference to this actor
self :: ActorIO m (ActorRef m)
self = ActorIO $ get >>= (return . ctxSelf)

-- | Obtain the reference to the parent of this actor
parent :: ActorIO m (ActorRef m)
parent = do
  ActorRef (ChildActorPath parent _) <- self
  return $ ActorRef parent

-- | Empty actor action
noop :: ActorIO m ()
noop = ActorIO $ return ()

-- | Obtain the reference to the sender of the currently processed message
sender :: ActorIO m (ActorRef m)
sender = ActorIO $ get >>= (return . ctxSender)

-- | Create a new child actor in the current context (actor system or actor).
actor :: Show m => String -- ^ The name of the actor. Must be url safe and unique within the context.
                -> (m -> ActorIO m ()) -- ^ The message handler of the actor.
                -> ActorIO m (ActorRef m) -- ^ An 'ActorRef' referencing the actor
actor name b = ActorIO $ do
  ctx <- get
  ref <- liftIO $ ctxActor ctx name (defaultBehavior { handleMessage = b })
  return ref

-- | Log message severity
data Severity = Debug | Info | Warn | Error deriving Show

-- | Log a message. Log messages will be sequentialized.
log :: Severity -- ^ The severity of the log message
    -> String  -- ^ The message to log
    -> ActorIO m ()
log s msg = ActorIO $ do
  ctx <- get  
  let path = actorPath $ ctxSelf ctx
  liftIO $ ctxSysMessage ctx $ Log path s msg

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
        let sysMessage m = writeChan system m
        let actor name b = do
              inbox <- runActor system b
              let path = (actorPath self) // name
              writeChan system $ NewActor path inbox
              return $ ActorRef path
        let context = ActorContext b sender self sysMessage actor
        let handler e = do
              let path = actorPath self
              let signal = Signal $ ChildFailed path (displayException (e :: SomeException))
              send $ Envelope self (ActorRef (parentPath path)) signal
              return Stopped
        let calculateNewBehavior = do
              let exec (ActorIO x) = execStateT x
              ctx <- exec (handleMsg bhv payload) context
              return $ ctxBehavior ctx
        newBehavior <- catch calculateNewBehavior handler
        loop newBehavior
  forkIO $ loop b
  return inbox

--------------------------------------------------------------------------------
-- Actor Systems                                                              --
--------------------------------------------------------------------------------

-- | An ActorSystem serves as the root of an actor hierarchy
data ActorSystem = ActorSystem {
  terminate :: IO () -- ^ Terminate the actor System
}

-- | Create a new actor system
actorSystem :: Show m => String -- ^ The name of the system. Must be url safe and unique within the process.
                      -> ActorIO m a -- ^ Action to initialize the system. Runs within the context of the root actor.
                      -> IO ActorSystem
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
              Schedule initialDuration duration smvar msg -> do
                forkIO $ do                  
                  threadDelay $ duration * 1000
                  stop <- tryTakeMVar smvar
                  when (isNothing stop) $ do
                    writeChan system $ Deliver msg
                    when (duration >= 0) $ writeChan system $ Schedule initialDuration duration smvar msg
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
  let sysMessage = writeChan system
  let actor name behavior = do
        inbox <- runActor system behavior
        let path = (actorPath rootRef) // name
        writeChan system $ NewActor path inbox
        return $ ActorRef path
  let rootContext = ActorContext Stopped rootRef rootRef sysMessage actor
  let stopMessage = Envelope rootRef rootRef $ Signal Stop
  let stop = writeChan system $ Deliver stopMessage
  let runInit (ActorIO init) = evalStateT init rootContext
  runInit init
  return $ ActorSystem stop
