# 001-pair-rdds-java
> creating `JavaPairRDD`s from Java Lists and `JavaRDD`s.

## Application Specs
+ Create a `JavaPairRDD` with the first letters of the alphabet with each pair being (letter, ordinal) as in ("a", 1), ("b", 2), etc.
+ Create a `JavaPairRDD` from an existing RDD in which the first element is the element from the RDD and the second one is "false".

## Concepts
Illustrates how to create `JavaPairRDD` using `JavaSparkContext.parallelize` and from an existing RDD using `mapToPair`.


## Notes
n/a