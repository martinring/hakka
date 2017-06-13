{-# LANGUAGE LambdaCase #-}
module Main where

import Hakka.Actor
import Prelude hiding (log)
import Control.Monad

data DiningHakkerMessage =
  Busy (ActorRef DiningHakkerMessage) |
  Put (ActorRef DiningHakkerMessage) |
  Take (ActorRef DiningHakkerMessage) |
  Taken (ActorRef DiningHakkerMessage) |
  Eat | Think deriving Show

chopstick i = actor ("chopstick" ++ show i) available where
  takenBy hakker = \case
    Take otherHakker -> self >>= (otherHakker !) . Busy
    Put h            -> if h == hakker then become available else noop
  available = \case
    Take hakker -> do
      become (takenBy hakker)
      self >>= (hakker !) . Taken
    Put h -> noop

hakker name left right = actor name receive where
  thinking = \case
    Eat -> do
      self <- self
      become hungry
      left ! (Take self)
      right ! (Take self)
  hungry = \case
    Taken stick ->
      if stick == left       then become $ waiting_for right left
      else if stick == right then become $ waiting_for left right
      else error $ "I didnt ask for stick " ++ show stick
    Busy stick -> do
      become denied_a_chopstick
  waiting_for w o = \case
    Taken w' ->
      if w == w' then do
        log Info $ name ++ " has picked up " ++ show left ++ " and " ++ show right ++ " and starts to eat."
        become eating
        self <- self
        self ! Think
      else error $ "Not waiting for stick " ++ show w'
    Busy cs -> do
      self >>= (o !) . Put
      startThinking 10
  denied_a_chopstick = \case
    Taken stick -> do
      self >>= (stick !) . Put
      startThinking 10
    Busy stick -> startThinking 10
  eating = \case
    Think -> do
      self <- self
      left ! (Put self)
      right ! (Put self)
      log Info $ name ++ " puts down his chopsticks and starts to think."
      startThinking 2000
  receive = \case
    Think -> do
      log Info $ name ++ " starts to think"
      startThinking 2000
  startThinking duration = do
    self <- self
    scheduleOnce duration self Eat
    become thinking


main :: IO ()
main = do
  (system,_) <- actorSystem "dinner" $ do
    chopsticks <- mapM chopstick [1 .. 5]
    let leftRight = zip chopsticks (tail chopsticks ++ [head chopsticks])
    let hakkerList = zip ["Ghosh","Boner","Klang","Krasser","Manie"] leftRight
    let createHakker (name,(l,r)) = hakker name l r
    hakkers <- mapM createHakker hakkerList
    mapM_ (! Think) hakkers
    log Info "initialized all hakkers"
  _ <- getLine
  terminate system