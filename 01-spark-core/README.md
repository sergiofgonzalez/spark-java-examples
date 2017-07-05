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