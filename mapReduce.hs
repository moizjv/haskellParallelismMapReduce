import Data.List (sort, group, sortBy, groupBy)
import Data.List.Split
import Control.Arrow ((&&&))
--import Control.Concurrent
import Control.Parallel
import Control.Parallel.Strategies


--MapReduce function takes Startegy for mapper function , mapper function, 
--strategy for reducer function ,reducer funtion and list of input
mapReduce :: Strategy b -> (a -> b) -> Strategy b -> ([b] -> b) -> [a] -> b
mapReduce mapStrat mapFunc reduceStrat reduceFunc input =
    mapResult `pseq` reduceResult
  where mapResult    = parMap mapStrat mapFunc input
        reduceResult = reduceFunc mapResult `using` reduceStrat

--Simple map reduce not used for parallelism
simpleMapReduce
    :: (a -> b)      -- map function
    -> ([b] -> c)    -- reduce function
    -> [a]           -- list to map over
    -> c
simpleMapReduce mapFunction reduceFunction input = reduceFunction (map mapFunction input)

--To split string based on spaces
stringSplitter str = splitOn " " str

--Mapper takes String and returns list of tuple containing word and its frequency.
mapper :: String -> [(String,Int)] 
mapper str = getCountOFWords (splitOn " " str)

-- Takes String and returns list of tuple containing word and its frequency
getCountOFWords :: [String] -> [(String,Int)]
getCountOFWords = map (head &&& length) . group . sort

--To sort and group which are used in reducer function
sortTuple (a1,b1) (a2,b2) = compare a1 a2
groupTuple (a1,b1) (a2,b2) =  a1 == a2

-- Used in reducer funtion for combining count of words
combining :: [(String,Int)] -> (String,Int)
combining ((word,value):xs) = (word, (value + summation xs))

-- Used in combining to get total count 
summation :: [(String,Int)] -> Int
summation [] = 0
summation ((word,value):xs) = value + summation xs

-- reducer function takes in list of list containing frequecy of words and gives integrated output of count of words
reducer :: [[(String,Int)]] -> [(String,Int)]
reducer list = map combining (groupBy groupTuple (sortBy sortTuple (concat list)))


 
--Main function takes in A,B and C txt files reads and passes it to map reduce
main = do       
       a   <- readFile "A.txt"
       b   <- readFile "B.txt"       
       c   <- readFile "C.txt"
--       putStrLn $ show $ mapReduce rpar mapper rseq reducer [a,b,c]
--     We pass rpar and rdeepseq in map function to make it NF and rseq we let is be WHNF
       putStrLn $ show $ mapReduce (rpar `dot` rdeepseq	) mapper rseq reducer [a,b,c]


