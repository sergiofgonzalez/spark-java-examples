# Adding records to a JavaRDD
> using `union` to add records to an existing `JavaRDD`

## Description
Illustrates how to add additional records to a `JavaRDD`:
+ arbitrary lines can be added with `JavaRDD.union` and `SparkContext.parallelize`
+ The records of an *RDD* can be added to another *RDD* using `JavaRDD.union`

In the example, we create a *RDD* with some lines and additional ones are created using both methods.

It is also demonstrated that the same approach can be used with `JavaPairRDD`.


## Concepts
+ Using `JavaRDD.union` append an *RDD* to another one
