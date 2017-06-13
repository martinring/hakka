{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

module Hakka.Actor (  
  -- * Actors
  ActorRef,

  -- * The ActorIO Monad
  ActorIO,  
  noop,
  liftIO,

  -- ** Creating Actors

  actor,

  -- ** Sending messages
  
  tell,
  (!),
  forward,
  schedule,
  scheduleOnce,

  -- ** Known ActorRefs

  sender,
  self,
  parent,

  -- ** Logging
  log,
  Severity (Debug,Info,Warn,Error),
  
  -- ** Changing the behavior

  become,
  stop,

  -- * Actor Systems
  actorSystem,
  ActorSystem, terminate, tellIO,

  -- * Util
  Cancellable, cancel

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

data Signal = PoisonPill | ChildFailed ActorPath String deriving Show

data Message m = Message m | Signal Signal deriving Show

data Envelope f t = Envelope {
  from :: ActorRef f,
  to :: ActorRef t,
  message :: Message t
} deriving (Show)

data SystemMessage = 
  forall m . NewActor {
    path :: ActorPath,
    inbox :: Inbox m
  } |
  ActorTerminated ActorPath |
  forall f t . Deliver (Envelope f t) |
  Log ActorPath Severity String |
  forall f t . Schedule Int Int (MVar ()) (Envelope f t) |
  forall m . TopLevelMessage (ActorRef m) m

--------------------------------------------------------------------------------
-- * Actors                                                                   --
--------------------------------------------------------------------------------

{- | 
  Actions which actors execute in response to a received message run within the
  ActorIO monad.
-}
newtype ActorIO t a = ActorIO (StateT (ActorContext t) IO a) 
  deriving (Monad,MonadIO,Applicative,Functor)

data Inbox t = forall f . Inbox (Chan (Envelope f t))

data ActorContext t = forall f. ActorContext {
  ctxBehavior :: Behavior t,
  ctxSender :: ActorRef f,
  ctxSelf :: ActorRef t,
  ctxSysMessage :: SystemMessage -> IO (),
  ctxActor :: forall a. Show a => String -> Behavior a -> IO (ActorRef a)
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
    PoisonPill -> stop
    ChildFailed path e ->
      log Error $ "child " ++ show path ++ " failed: " ++ e
}

-- | Send a message to an actor. Does not block (immediately returns control flow).
tell :: ActorRef m -- ^ The recipient of the message
     -> m  -- ^ The message to send
     -> ActorRef f -- ^ The sender of the message
     -> ActorIO m ()
tell other msg sender = ActorIO $ do
  ctx <- get
  liftIO $ ctxSysMessage ctx $ Deliver $ Envelope sender other (Message msg)

-- | Like 'tell' but the sender will be the actor which executes the action.
(!) :: ActorRef m -- ^ The reference to the receiving actor
    -> m -- ^ The message
    -> ActorIO m ()
ref ! msg = self >>= tell ref msg
  
-- | Like 'tell' but the sender will be the sender of the currently processed message.
{-forward :: ActorRef m -- ^ The reference to the receiving actor
        -> m -- ^ The message
        -> ActorIO m ()
forward ref msg = sender >>= tell ref msg-}

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

-- | Obtain the reference to the actor executing the action
self :: ActorIO m (ActorRef m)
self = ActorIO $ get >>= (return . ctxSelf)

-- | Empty actor action
noop :: ActorIO m ()
noop = ActorIO $ return ()

-- | Create a new child actor in the current context (actor system or actor).
actor :: Show a => String -- ^ The name of the actor. Must be url safe and unique within the context.
                -> (a -> ActorIO a ()) -- ^ The message handler of the actor.
                -> ActorIO m (ActorRef a) -- ^ An 'ActorRef' referencing the actor
actor name b = ActorIO $ do
  (ActorContext _ _ _ _ actor) <- get
  ref <- liftIO $ actor name (defaultBehavior { handleMessage = b })
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

runActor :: Show m => Chan SystemMessage -> Behavior m -> IO (Inbox m)
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
              send $ Envelope (self) (ActorRef (parentPath path)) signal
              return Stopped
        let calculateNewBehavior = do
              let exec (ActorIO x) = execStateT x
              ctx <- exec (handleMsg bhv payload) context
              return $ ctxBehavior ctx
        newBehavior <- catch calculateNewBehavior handler
        loop newBehavior
  forkIO $ loop b
  return $ Inbox inbox

--------------------------------------------------------------------------------
-- Actor Systems                                                              --
--------------------------------------------------------------------------------

-- | An ActorSystem serves as the root of an actor hierarchy
data ActorSystem = forall m . ActorSystem {  
  terminate :: IO (), -- ^ Terminate the actor System  
  tellIO :: ActorRef m -> m -> IO () -- ^ For interacting with an actor from outside an actor system
}

data InternalActorRef = InternalActorRef ActorPath TypeRep (Inbox m)

data ActorSystemState = EmptySystem

-- | Create a new actor system
actorSystem :: Show m => String -- ^ The name of the system. Must be url safe and unique within the process.
                      -> ActorIO m a -- ^ Action to initialize the system. Runs within the context of the root actor.
                      -> IO (ActorSystem, a)
actorSystem name init = do
  system <- newChan
  let rootRef = ActorRef (RootActorPath $ validateSegment name)
  let loop state = do
        msg <- readChan system        
        let deliver msg@(Envelope from to payload)
              | to == rootRef = case payload of
                  Signal PoisonPill -> do                    
                    putStrLn "System Stopped"                    
                  Signal (ChildFailed p e) -> do
                    putStrLn $ "Actor Failed: " ++ show p ++ ": " ++ e
                    writeChan system $ ActorTerminated p                    
                  Message m -> do
                    writeChan system $ TopLevelMessage from m
              | otherwise = case lookup (actorPath to) (actors state) of
                  Just (Inbox recipient) -> do
                    writeChan recipient msg
                  Nothing -> do
                    putStrLn $ "message could not be delivered to " ++ (show (actorPath to))
        let calculateNewState = case msg of
              NewActor path inbox -> do
                return $ Just $ state { actors = ((path,inbox):(actors state)) }
              ActorTerminated path -> do
                let (remove,retain) = partition ((`isChildOf` path) . fst) (actors state)
                forM_ remove $ \(p,a) -> writeChan a $ Envelope rootRef (ActorRef p) (Signal PoisonPill)
                return $ Just $ state { actors = retain }
              Schedule initialDuration duration smvar msg -> do
                forkIO $ do                  
                  threadDelay $ duration * 1000
                  stop <- tryTakeMVar smvar
                  when (isNothing stop) $ do
                    writeChan system $ Deliver msg
                    when (duration >= 0) $ writeChan system $ Schedule initialDuration duration smvar msg
                return $ Just state
              Log path s msg -> do
                putStrLn $ "[" ++ show s ++ "] [" ++ show path ++ "]: " ++ msg
                return $ Just state
              Deliver msg -> do
                deliver msg
                return $ Just state              
              TopLevelMessage from msg -> do
                putStrLn $ "plain message delivered to root actor from " ++ (show from) ++ ": " ++ (show msg)
                return $ Just state
        let handler e = do
              putStrLn $ "Actor system '" ++ name ++  "' failed: " ++ (displayException (e :: SomeException))
              return Nothing
        newState <- catch calculateNewState handler
        maybe (return ()) loop newState
  forkIO $ loop $ ActorSystemState []
  let sysMessage = writeChan system
  let actor name behavior = do
        inbox <- runActor system behavior
        let path = (actorPath rootRef) // name
        writeChan system $ NewActor path inbox
        return $ ActorRef path
  let rootContext = ActorContext Stopped rootRef rootRef sysMessage actor
  let stopMessage = Envelope rootRef rootRef $ Signal PoisonPill
  let stop = writeChan system $ Deliver stopMessage
  let runInit (ActorIO init) = evalStateT init rootContext
  let tellIO ref msg = writeChan system (Deliver $ Envelope rootRef ref (Message msg))
  result <- runInit init
  return $ (ActorSystem stop tellIO,result)
