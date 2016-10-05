import           Disorder.Core.Main

import qualified Test.IO.Spine.Schema as Schema

main :: IO ()
main =
  disorderMain [
      Schema.tests
    ]
