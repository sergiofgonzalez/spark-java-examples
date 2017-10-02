# Using Spark Core to Count the Word Occurrences in a File
> using Spark Core API to count the words on a text file downloaded from an HTTP address

## Application Specs
The application counts the words found in a file downloaded from the internet using Spark core API. 

Note that the sorting of the resulting list of words and occurrences is performed on a materialized `Map`. For an example on which the sorting is performed using *Spark Core API* please review [03 &mdash; Processing a structured Purchase Log with Spark Core](../003-complimentary-customer-gifts/).

## Concepts
+ Using the `SparkApplicationTemplate` for a Spark Core application (SparkSQL is added as a dependency, but never used)
+ Loading a file into an RDD using `JavaSparkContext.textFile`
+ Using `JavaRDD.flatMap` to create a `JavaRDD` whose elements are the words found in the file
+ Using `JavaPairRDD.countByKey()` to materialize a `JavaPairRDD` as a `Map` 
+ Sorting a `Map` by value and collecting the result as a `Map` using `collect(toMap(...))`

