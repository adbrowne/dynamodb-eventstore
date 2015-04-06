module Main where

import           Data.Text.Lazy (Text, pack)
import           Web.Scotty
import           Webserver      (app)
import           Control.Monad.IO.Class (liftIO)

showEvent :: Show a => a -> ActionM ()
showEvent a = do
  liftIO $ print "SomeIo"
  (html . pack . show) a

main = scotty 3000 (app showEvent)
