{-# LANGUAGE OverloadedStrings #-}
module WebserverInternalSpec (spec) where


import           BasicPrelude
import qualified Data.Text.Lazy               as TL
import qualified DynamoDbEventStore.Webserver as W
import           Test.Tasty.Hspec
import           Test.Tasty.QuickCheck
import           Text.Read                    (readMaybe)

showText :: Show a => a -> LText
showText = TL.fromStrict . show

spec :: Spec
spec = do
  describe "parseInt64" $ do
    let run = W.runParser W.positiveInt64Parser ()

    it "can parse any positive int64" $ property $
      \x -> x >= 0 ==> run (showText (x :: Int64)) === Right x

    it "will not parse negative numbers" $ property $
      \x -> x < 0 ==> run (showText (x :: Int64)) === Left ()

    it "will not parse anything that read cannot convert read" $ property $
      \x ->  (Text.Read.readMaybe x :: Maybe Int64) == Nothing ==> run (showText x) === Left ()

    it "will not parse numbers that are too large" $ do
      let tooLarge = toInteger (maxBound :: Int64) + 1
      run (showText tooLarge) == Left ()
  describe "global feed position" $ do
    it "can round trip any position" $ property $
      \position -> W.parseGlobalFeedPosition(W.globalFeedPositionToText(position)) === Just position
