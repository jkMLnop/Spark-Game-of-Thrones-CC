from pyspark import SparkContext, SparkConf
import re

#GENERAL:
# look into 'raise' instead of except
# streamline processing into basically one step

#higher order function to parse out individual words in a file's contents and 
#make lowercase then load into a set

def cleanup_words(file_and_contents):
	#TODO-ASK ADRI: figure out how to use '.distinct()' - may require use of a tuple..

	split_contents = set()
	try:
	 	#break file and contents into tuple of file paths and contents

		filename,contents = file_and_contents

		#splits contents on non-words, result is a set of all unique words
		#NOTE: elements in a set are atomic - so we eliminate duplicates
		#NOTE: alternative would be using .distinct() but that might be slower
				
		split_contents = set(re.split("\W+", contents.lower()))

		return(split_contents,filename)
	
	except OSError:
		print("Cleanup Error - problem encountered while attempting to clean file contents")

		return

#higher order function setting up for eventually reducing by key

def map_filename_to_word(word_file):
	words,filename = word_file

	try:
		#extract numeric filename from absolute filepath and cast to int
		
		filename_int = int(filename.rsplit('/', 1).pop())
		
		mapped_word_files = ()

		for word in words:
			#exclude empty strings from further processing
			
			if word == "":
				continue
			mapped_word_files += (word,filename_int)

			return(mapped_word_files)

	except TypeError:
		print("File Naming Error: input file name non-numeric")

def process(input_file):
	#ADRI SAID:
	# make it just a one-liner broken up into multiple lines, but not instantiating new RDDs for each step, keep comments!

        #spark context setup

	spark_context = SparkContext(appName="reverse-index").getOrCreate()

	#initialize PairRDD (of key-value pairs) where keys are filepaths &
	#values are the file contents

	file_rdd = spark_context.wholeTextFiles(input_file)	#wholeTextFiles returns an array of arrays but keeps the name of the file which we need here

	#perform basic cleanup file contents (remove punctuation & make lower case)

	words_rdd = file_rdd.map(cleanup_words)
	#print(words_rdd.take(1))

	#map word sets for each file to their corresponding filenames

	mapped_words_rdd = words_rdd.map(map_filename_to_word)
	print(mapped_words_rdd.take(10))
	
	#NOTE groupByKey alone will return the memory address of an iterable but 
	     #we need a map with a lambda function to parse the iterable into a list		

	files_grouped_by_word = mapped_words_rdd.groupByKey().map(lambda a : (a[0], sorted(list(a[1]))))
	print(files_grouped_by_word.take(10))	

	
	#all_files_per_word_rdd = flatmapped_words_rdd.reduceByKey(lambda b, c : b + c)	
	#print(all_files_per_word_rdd.take(1))

def main(input_file, output_file):
	process(input_file)
	print("END OF MAIN REACHED")


if __name__ == '__main__':
	#define i/o file locations

	input_location = '../input/*'
	output_location = '../output/'

	#pass file paths to main

	main(input_location, output_location)
