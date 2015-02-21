module Main where

import Web.Scotty
import Webserver (app)

main = scotty 3000 app
