# Spark-Game-of-Thrones-CC

## Spark Processing:
### Functions Used: 
```
wholeTextFiles()
map()
groupByKey()
```

```wholeTextFiles()``` reads a directory containing multiple text files and returns them as an **RDD** of (filename,content) key-value pairs (meaning each file constitutes a single record). An alternative would be using ```textFile()``` to extract data from files but it would return one record per line of each file so doesnt suit our needs. Note: this happens in the driver. 

```map()``` is the most basic Spark **transformation**, when used it will return a new **RDD** of the same size as the input **RDD**, it passes each element of the source through whichever function it has as a parameter.

```groupByKey()``` is another Spark **transformation**, when called on a dataset of key-value (K,V) pairs it returns a set of (K,iterable<V>) pairs (in our case a list of files within which the word is found!). 
 **TODO: talk about reduceByKey() and whether its a better option!** 

### Key Concepts:
**Resilient Distributed Datasets (RDDs)** a collection of elements partitioned across nodes which can be operated on in parallel. 

**Transformations** in Spark happen in a distrubuted/parallelized way, they are not computed until an **action** is encountered requiring data being returned back to the driver program.

**Actions** return value to the driver program after running computation on an **RDD**. 
