# 001-github-activity-analysis-v1

## Application Specs
This program loads the GitHub activity for a set of days on a dataset and performs a basic set of descriptive analytics operations on it:
+ Filter the contents of the dataset so that only events of type *Push* remain
+ Counts the total number of events and those that are *Push*
+ Groups the *Push* events by user, and sorts the result
+ Filters the *Push* events by user dataset, so that only a custom set of users are considered

## Concepts
+ Creating a `SparkSession`
+ Loading a set of JSON files from the local file system
+ Using `Dataset<Row>` datasets
+ Using `Dataset<Row>#count`
+ Filter the contents of a `Dataset<Row>`
+ Grouping `Dataset<Row>` contents using `groupBy`
+ Sorting a `Dataset<Row>` using `orderBy` and `desc()`
+ Defining Broadcast variables (i.e. Java object that can be efficiently distributed to workers) using `SparkContext.broadcast`.
+ Defining a UDF (i.e. a custom piece of Java logic that can be invoked within Spark functions like `.filter`)
+ Invoking a UDF inside a SparkSQL `filter` operation

## Execution Notes
+ The application assumes that the GitHub activity has been previously downloaded using:
```
$ mkdir /tmp/github-data
$ cd /tmp/github-data
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
$ gunzip *
```
This example is **NOT** prepared to be run using `spark-submit`.
+ You can run it from Eclipse as a regular Java application &mdash; it takes around 30 secs to process on a somewhat powerful laptop.

