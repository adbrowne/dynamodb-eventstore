{-# LANGUAGE OverloadedStrings, QuasiQuotes #-}
module Main (main) where

import           Test.Hspec
import           Test.Hspec.Wai
import           Network.Wai.Test (SResponse)
import           Network.HTTP.Types (methodPost)
import           Network.Wai (Application)
import           Data.Aeson (Value(..), object, (.=))
import qualified Webserver as W
import qualified Web.Scotty as S
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.Text.Encoding as T
import qualified Network.HTTP.Types as H

main :: IO ()
main = hspec spec

printResponse :: WaiSession SResponse -> WaiExpectation
printResponse action = do
  r <- action
  error $ show r
      
addEventPost headers = 
    request methodPost "/streams/streamId" headers BL.empty 
spec :: Spec
spec = with (S.scottyApp W.app) $ do
  let requestWithExpectedVersion = addEventPost [("ES-ExpectedVersion", "1")] 
  let requestWithoutExpectedVersion = addEventPost []
  let requestWithoutBadExpectedVersion = addEventPost [("ES-ExepectedVersion", "NotAnInt")]
      
  describe "POST /streams/streamId" $ do
    it "responds with 200" $ do
      requestWithExpectedVersion `shouldRespondWith` 200

    it "responds with body" $ do
      requestWithExpectedVersion `shouldRespondWith` "StreamId:streamId expectedVersion:1"

  describe "POST /streams/streamId without ExepectedVersion" $ do
    it "responds with 400" $ do
      requestWithoutExpectedVersion `shouldRespondWith` 400

  describe "POST /streams/streamId with non-integer ExpectedVersion" $ do
    it "responds with 400" $ do
      requestWithoutBadExpectedVersion `shouldRespondWith` 400
