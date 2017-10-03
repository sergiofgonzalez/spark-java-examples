# Sorting a JavaPairRDD by value
> swapping value and key in a JavaPairRDD to sort its contents by value 

## Description
Spark does not provide a sort by value operation for `JavaPairRDD`, but you can transform the original *pair RDD* swapping key and value and then using `sortByKey`.

In the example, we illustrate how to perform that approach with a simple *pair RDD* and with a more complex one in which we do the sorting by one of the individual fields within the value.


## Concepts
+ Using `JavaPairRDD.sortByKey`
