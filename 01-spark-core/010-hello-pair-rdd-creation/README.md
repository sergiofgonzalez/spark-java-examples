# Creating JavaPairRDD instances
> Creating `JavaPairRDD`s from Java Lists and `JavaRDD`s

## Description
Pair RDDs are used to represent and process key-value pairs in Spark. To work with Pair RDDs in Java you have to use the `JavaPairRDD` class.

The example illustrates how to create `JavaPairRDD`:
+ Using `parallelizePairs` method which accepts a list of `Tuple2` elements
+ Using `mapToPair` transformation on an existing `JavaRDD`


## Concepts
+ Using the `parallelizePairs` to create a `JavaPairRDD`
+ Using `mapToPair` on an existing `JavaRDD` to generate a `JavaPairRDD`
+ Introducing `Tuple2<T,V>` to represent a key-value element
