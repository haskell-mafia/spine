import           Disorder.Core.Main

import qualified Test.Spine.Memory as Memory

main :: IO ()
main =
  disorderMain [
      Memory.tests
    ]
