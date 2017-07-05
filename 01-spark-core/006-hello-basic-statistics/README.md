# 006-hello-basic-statistics

## Application Specs

This program illustrates how to obtain basic statistics from a `JavaDoubleRDD` and how to create a `JavaDoubleRDD` from a `JavaRDD`.


## Concepts

The `JavaDoubleRDD` features a series of methods that compute basic statistics from its elements:
+ `mean`
+ `sum`
+ `count`
+ `variance`
+ `stdev`
+ `min`
+ `max`
+ `stats` &mdash; returns a `StatCounter` object with the previous information

In order to obtain a `JavaDoubleRDD` you can either:
+ use `parallelizeDoubles` method
+ use the `JavaRDD.mapToDouble` method 


## Notes

+ The program uses the `IntListBuilder` class from the support package ([001-int-list-builder](../../00-support/001-int-list-builder/).
+ It is intended to be used in a non-clustered Spark installation.