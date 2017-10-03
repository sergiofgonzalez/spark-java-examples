# 01 &mdash; Spark Core API
> Illustrates basic usage of Spark RDDs and its transformations and actions

## [01 &mdash; Hello Spark](./001-hello-spark/)
Serves as a shakedown test for the local development environment. In the example, the Spark license file is loaded and the number of lines in this file are counted.

## [02 &mdash; Hello `filter`](./002-hello-filter/)
Illustrates how to use the `JavaRDD.filter` method used to filter out records from an *RDD*. In the example, we demonstrate how to filter the lines from an `JavaRDD<String>` that match a particular string.

## [03 &mdash; Hello `map`](./003-hello-map/)
Illustrates how to use the `JavaRDD.filter` method used to filter out records from an *RDD*. In the example, we demonstrate how to filter the lines from an `JavaRDD<String>` that match a particular string.

## [04 &mdash; Hello `flatMap`](./004-hello-flatmap/)
Illustrates how to use the `JavaRDD.flatmap` method used to flatten an RDD whose elements are also collections.

## [05 &mdash; Hello Sampling](./005-hello-sampling/)
Illustrates how to use the `JavaRDD.sample` transformation and the `JavaRDD.takeSample` action.

## [06 &mdash; Hello Basic Statistics](./006-hello-basic-statistics/)
Illustrates how to obtain basic statistics (such as the mean, stdev, etc.) from a `JavaDoubleRDD`.

## [07 &mdash; Hello Histograms](./007-hello-histograms/)
Illustrates how to obtain a histogram from the data inside a `JavaDoubleRDD`.

## [08 &mdash; Hello Approx Sum and Mean](./008-hello-approx-sum-and-mean/)
Illustrates how to obtain an approximate sum and mean from a `JavaDoubleRDD`.

## [09 &mdash; Hello RDD creation](./009-hello-rdd-creation/)
Illustrates how to create `JavaRDD`.

## [10 &mdash; Hello PairRDD creation](./010-hello-pair-rdd-creation/)
Illustrates how to create `JavaPairRDD`.

## [11 &mdash; Hello parsing delimited files](./011-hello-parsing-delimited-files/)
Illustrates how to load the contents of a file and parse its contents manually.

## [12 &mdash; Changing the list of values associated to a key with JavaPairRDD.flatMapValues](./012-adding-values-to-pair-rdd-with-map-values/)
Illustrates how to use `flatMapValues` to change the number of elements associated to a given key when working with `JavaPairRDD`.

## [13 &mdash; Adding records to an RDD](./013-adding-records-to-an-rdd/)
Illustrates how to use `union` to add records to a `JavaRDD` or `JavaPairRDD`.

## [14 &mdash; Sorting a PairRDD by Value](./014-sorting-pair-rdd-by-value/)
Illustrates how to sort a `JavaPairRDD` by the value, or by a field of the value.

## [15 &mdash; Hello saving an RDD](./015-hello-save-rdd/)
Illustrates how to save an RDD on the file system.


