from pyspark import SparkContext, SparkConf
import re

#higher order function to parse out individual words in a file's contents and 
#make lowercase then load into a set
def cleanup_words(file_and_contents):
	#TODO-ASK PETER: figure out how to use '.distinct()' - may require use of a tuple..
	split_contents = set()
	try:
	 	#break file and contents into tuple of file paths and contents
		filename,contents = file_and_contents

		#splits contents on non-words, result is a set of all unique words
		#NOTE: elements in a set are atomic - so we eliminate duplicates
		#NOTE: alternative would be using .distinct() but that might be slower
		split_contents = set(re.split("\W+", contents.lower()))

		return(split_contents,filename)
	except:
		print("Cleanup Error - problem encountered while attempting to clean file contents")

		return

#higher order function setting up for eventually reducing by key
def map_filename_to_word(word_file):
	words,filename = word_file

	try:
		#extract numeric filename from absolute filepath and cast to int
		filename_int = int(filename.rsplit('/', 1).pop())

		#TODO THIS IS BAD AND NEEDS TO BE CHANGED!! TUPLES ARE IMMUTABLE
		mapped_word_files = ()

		for word in words:
			#exclude empty strings from further processing
			if word == "":
				continue
			#TODO USING A LIST ALREADY SO DO IT BETTER!
			#TODO SQUARE BRACKET IN SPECS?
			mapped_word_files += ((word,[filename_int]),)

			return(mapped_word_files)

	except TypeError:
		print("File Naming Error: input file name non-numeric")

def process(input_file):
        #spark context setup
	spark_context = SparkContext(appName="reverse-index").getOrCreate()

	#initialize PairRDD (of key-value pairs) where keys are filepaths &
	#values are the file contents
	file_rdd = spark_context.wholeTextFiles(input_file)

	#perform basic cleanup file contents (remove punctuation & make lower case)
	words_rdd = file_rdd.map(cleanup_words)
	#print(words_rdd.take(1))

	#map word sets for each file to their corresponding filenames
	mapped_words_rdd = words_rdd.map(map_filename_to_word)
	print(mapped_words_rdd.take(10))
	#NOTE WORKS UNTIL HERE!
	
	#flatmap individual words to filenames - each word to its filename
	#setting up for reduceByKey, words are the keys, filenames are the values
	flatmapped_words_rdd = mapped_words_rdd.flatMap(lambda a : a)
	print(flatmapped_words_rdd.take(1))
	#NOTE - ^^MIGHT ACTUALLY BE WORKING!
			
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
