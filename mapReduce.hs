import Data.List (sort, group, sortBy, groupBy)
import Data.List.Split
import Control.Arrow ((&&&))
--import Control.Concurrent
import Control.Parallel
import Control.Parallel.Strategies


mapReduce :: Strategy b -> (a -> b) -> Strategy b -> ([b] -> b) -> [a] -> b
mapReduce mapStrat mapFunc reduceStrat reduceFunc input =
    mapResult `pseq` reduceResult
  where mapResult    = parMap mapStrat mapFunc input
        reduceResult = reduceFunc mapResult `using` reduceStrat


simpleMapReduce
    :: (a -> b)      -- map function
    -> ([b] -> c)    -- reduce function
    -> [a]           -- list to map over
    -> c
simpleMapReduce mapFunction reduceFunction input = reduceFunction (map mapFunction input)

stringSplitter str = splitOn " " str


mapper :: String -> [(String,Int)] 
mapper str = getCountOFWords (splitOn " " str)

getCountOFWords :: [String] -> [(String,Int)]
getCountOFWords = map (head &&& length) . group . sort


sortTuple (a1,b1) (a2,b2) = compare a1 a2
groupTuple (a1,b1) (a2,b2) =  a1 == a2

combining :: [(String,Int)] -> (String,Int)
combining ((word,value):xs) = (word, (value + summation xs))


summation :: [(String,Int)] -> Int
summation [] = 0
summation ((word,value):xs) = value + summation xs

reducer :: [[(String,Int)]] -> [(String,Int)]
reducer list = map combining (groupBy groupTuple (sortBy sortTuple (concat list)))


 

main = do       
       a   <- readFile "A.txt"
       b   <- readFile "B.txt"       
       c   <- readFile "C.txt"
--       putStrLn $ show $ mapReduce rpar mapper rseq reducer [a,b,c]
       putStrLn $ show $ mapReduce (rpar `dot` rdeepseq	) mapper rseq reducer [a,b,c]


