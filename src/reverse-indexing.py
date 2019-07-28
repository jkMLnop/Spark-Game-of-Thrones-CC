from pyspark import SparkContext, SparkConf
import re

def cleanup_words(file_and_contents):
	split_contents = set()
	try:
		#NOTE: THIS IS NOT WORKING AT ALL!
	 	#break file and contents into tuple of file paths and memory address housing contents!
		filename,contents = file_and_contents
		print(filename)
		print(contents)
		#split_contents = set(re.split("\W+", body.lower()))
	except:
		print("Cleanup Error - problem encountered while attempting to clean file contents")
	#return(split_contents,filename)
	return(split_contents,"blah")

def process(input_file):
        #spark context setup
	spark_context = SparkContext(appName="reverse-index").getOrCreate()

	#initialize PairRDD (of key-value pairs) where keys are filepaths &
	#values are the file contents
	file_rdd = spark_context.wholeTextFiles(input_file)

	print("FILE RDD CREATED")

	files = file_rdd.collectAsMap()
	for file,contents in files.items():
		print(file)
		print(contents[:5])
	#NOTE: Gets to here then breaks down for some reason!

	#perform basic cleanup file contents (remove punctuation & make lower case)
	#cleanup_words(files)

def main(input_file, output_file):
	process(input_file)
	print("END OF MAIN REACHED")

if __name__ == '__main__':
	#define i/o file locations
	input_location = '../input/*'
	output_location = '../output/'

	#pass file paths to main
	main(input_location, output_location)
