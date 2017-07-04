# 001-hello-spark

## Application Specs
This program loads the Apache Spark License file, counts the number of lines in that file and display the count on the console.

## Concepts
+ Creating a `JavaSparkContext`
+ Loading a text file from the local file system
+ Using `JavaRDD.count` method

## Notes
+ The program assumes that `$SPARK_HOME` environment variable is correctly pointing to a local installation of Spark.
+ It is intended to be used in a non-clustered Spark installation.
