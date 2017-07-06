# 007-hello-histograms

## Application Specs

This program illustrates how to obtain a histogram from the data of a `JavaDoubleRDD`.


## Concepts

Histograms are used for graphical representation of data, with X axis has value intervals and Y axis has the data density (number of elements in the corresponding intervals).

+ `JavaDoubleRDD.histogram(double[] intervals) : long[]` &mdash; create the histogram according to the given intervals and returns the number of values in those given intervals.
+ `JavaDoubleRDD.histogram(int numIntervals) : Tuple2<double[],long[]>` &mdash; create the histogram by creating the given number of equal intervals.

The intervals in both cases include the lower limit and excludes the higher one:
```
JavaDoubleRDD = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
histogram({0.0, 3.33, 6.66, 10.0}) => 
[0.0, 3.33) : 4
[3.33, 6.66): 3
[6.66, 10.0): 3

histogram(5) =>
[0.0, 1.8): 2
[1.8, 3.6): 2
[3.6, 5.4): 2
[5.4, 7.2): 2
[7.2, 9.0): 2
```


## Notes

+ It is intended to be used in a non-clustered Spark installation.