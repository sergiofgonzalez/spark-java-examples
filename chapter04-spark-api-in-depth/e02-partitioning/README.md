# e02-partitioning
> working with advanced partitioning

# Application Specs
Using a data file with the format:

```
site_name,timestamp,val1,val2,val3,val4,val5,val6
```

Obtain:
+ Create a JavaPairRDD in which the key is the `site_name` and the value is the complete record.
+ Use the `glom` transformation to coalesce the data into a list whose each of the elements represent the data in each of the partitions, so that the result of the `glom` is an RDD with as many elements as partitions in the original RDD.
+ Create a custom partitioner that associates each key with a particular site.
+ Use the `mapPartitionsToPair` to apply a function to the elements of a given partition.
+ Use the `reduceByKeyLocally` which merges the value for each key and returns the result as a Map.

# Concepts
Illustrates some advanced partitioning concepts:
+ `glom` transformation which returns an RDD created by coalescing all elements within each partition into an array.
+ `mapPartitionsToPair` which applies a function to each partition of an RDD
+ Custom `Partitioner` for a JavaPairRDD that arranges the elements of a PairRDD according to a custom strategy.
+ `reduceByKeyLocally` which merges the value for each key and returns the result as a Map.

# Notes
n/a