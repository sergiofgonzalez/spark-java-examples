# 008-hello-approx-sum-and-mean

## Application Specs

This program illustrates how to obtain an approximate sum and mean from the data of a `JavaDoubleRDD`.


## Concepts

+ `JavaDoubleRDD.approximateSum(Long timeoutMillis, double confidence)` 
+ `JavaDoubleRDD.histogram(int numIntervals) : Tuple2<double[],long[]>`

## Notes

+ It is intended to be used in a non-clustered Spark installation.