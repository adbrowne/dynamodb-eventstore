module Main where

import           Data.Text.Lazy (Text, pack)
import           Web.Scotty
import           Webserver      (app)

showEvent :: Show a => a -> ActionM ()
showEvent a = (html . pack . show) a

main = scotty 3000 (app showEvent)
