# 002-hello-filter

## Application Specs
This program loads the Apache Spark License file, counts the number of lines in that file matching a given string and display the count and the lines themselves on the console.

## Concepts
+ Using `JavaRDD.filter(Function<String,Boolean> filterFn)` method
  + Defining the *filterFn* function as an inline anonymous lambda
  + Defining the *filterFn* as a named lambda
  + Creating a *filterFn* factory to parameterize the string to match using lambdas and old-style functions

## Notes
+ The program assumes that `$SPARK_HOME` environment variable is correctly pointing to a local installation of Spark.
+ It is intended to be used in a non-clustered Spark installation.
