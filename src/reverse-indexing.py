from pyspark import SparkContext, SparkConf
import re

def cleanup_words(file_and_contents):
	split_contents = set()
	try:
		#NOTE: THIS IS NOT WORKING AT ALL!
	 	#break file and contents into tuple of file paths and contents
		filename,contents = file_and_contents

		#TODO: find a better way to do this?
		split_contents = set(re.split("\W+", body.lower()))

		return(split_contents,filename)
	except:
		print("Cleanup Error - problem encountered while attempting to clean file contents")

		return

#here we are setting ourselves for eventually reducing by key
def map_filename_to_word(word_file):
	words,filename = word_file

	#extract numeric filename from absolute filepath and cast to int
	filename_int = int(filename.rsplit('/', 1).pop())

	print(str(filename_int))

def process(input_file):
        #spark context setup
	spark_context = SparkContext(appName="reverse-index").getOrCreate()

	#initialize PairRDD (of key-value pairs) where keys are filepaths &
	#values are the file contents
	file_rdd = spark_context.wholeTextFiles(input_file)

	#perform basic cleanup file contents (remove punctuation & make lower case)
	words_rdd = file_rdd.map(cleanup_words).take(5)

	#TODO: figure out the error trapping logic here...
	if words_rdd:
		mapped_words_rdd = words_rdd.map(map_filename_to_word)

	#NOTE: HOW DO I FIGURE OUT IF ITS BEING SEPERATED OR NOT!?

def main(input_file, output_file):
	process(input_file)
	print("END OF MAIN REACHED")

if __name__ == '__main__':
	#define i/o file locations
	input_location = '../input/*'
	output_location = '../output/'

	#pass file paths to main
	main(input_location, output_location)
