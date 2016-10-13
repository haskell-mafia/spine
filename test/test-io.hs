import           Disorder.Core.Main

import qualified Test.IO.Spine.Data as Data
import qualified Test.IO.Spine.Schema as Schema

main :: IO ()
main =
  disorderMain [
      Data.tests
    , Schema.tests
    ]
