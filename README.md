# NGram-Hadoop
## Introduction
This project focuses on generating n-grams for a chosen collections of contemporary novels downloaded on Gutenberg website 1, using Suffix-σ algorithm which relies on siring and aggregating suffixes. After obtaining the n-grams list for the document collections, we will use the frequency to predict the 3 most likely 4th words that form a 4-word phrase from our document set, given a phrase of 3 words.

## Program Prases
The program has two major phases: the first for Suffix-σ algorithm to obtain the n-gram list, the second to test the program, namely to predict the 3 most likely 4th words that form a 4- word phrase with a given 3 word phrase.

### Suffix-σ Algorithm Phase
This phase has in total four Map/Reduce tasks. Here we referenced the design from the creator of Suffix-σ algorithm. 2
The first mapper/reducer takes the document corpus and convert it into a word dictionary. A line of the dictionary would be in the format of “word-frequencyRank-frequency”, such as “apple 45 11111”.
The second mapper/reducer (actually no reducer task) converts the word dictionary into a IntArrayWritable data structure, which is created for further suffix processing with suffix Array.
The third mapper/reducer takes the IntArrayWritable data and convert it into IntArrayWritable as an output key and IntWritable as an output value.
The fourth (last) mapper/reducer in this phase converts the IntArrayWritable data structure into Text back.
1 https://www.gutenberg.org
2 https://github.com/kberberi/mpiingrams
      1
 ## Test Program Phase
After the Suffix-σ Algorithm is performed and the list of n-grams in the documents is returned, we conduct another mapper/reducer task to rank the 4-grams containing the given 3-word phrase by the frequency of appearance. The top three 4-grams are returned and we can find out the three most likely 4th word for the given 3-word phrase.
Team Own Implementation of 4-Gram
Besides the implementation of Suffix-σ which referenced the algorithm creator’s design, we also implemented our own way to find 4-grams for this task. We use only a mapper/reducer to tokenize every word then read every four word in a queue and output the four grams and their count to an output file. Then we can use the same mapper/reducer in the Test Program Phase mentioned above to find out the most likely 4th word.
### How to Run the Program
Copy the data file containing all the novels (or other dataset you’d like to test out) in to HDFS directory, <INPUT_DIRECTORY>, into cluster. Copy the runnable jar file into cluster. The program takes in total 4 arguments, with the first to be the <INPUT_DIRECTORY> , and the following 3 arguments to be the 3 words compositing the 3-word phrase. Run command in the directory where you put the runnable jar file for this program:
$ hadoop jar SuffixSigma.jar <INPUT_DIRECTORY> <TheFirstWord> <TheSecondWord>
<TheThirdWord>
The output file would be stored in the HDFS directory <../FinalMatchingResult> and the 3 most likely 4th words would be written in the output file under this directory.
## Scalability Consideration
The current implementation is scalable. The program is set to process documents of arbitrary size. The current dataset used by the program is 50MB, yet the program should be able to process larger dataset if provided. Therefore, scalability is not issue for this program.
## Further Enhancement
Reduce the mappers and reducers number to simplify the process; use more advanced matching methods to conduct fuzzy logic search to match the three words phrase to the four grams; save the four gram output and do the prediction directly from mapping the output file; allow the user to choose from different n-gram algorithms to conduct the task.
