# Parsing Delimited Files
> Creating `JavaRDD`s from delimited files

## Description
Use `textFile` and `JavaPairRDD.map` methods to load and parse delimited files.

In the example, a text file in which each line represents a record delimited by `#` is parsed and loaded into a `JavaPairRDD`.

## Concepts
+ Using `JavaSparkContext.textFile` to load a file into a *flat* (i.e. without structure `JavaRDD`).
+ Using `JavaRDD.map` and `String.split` to split the contents of a line
