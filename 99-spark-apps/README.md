# 99 &mdash; Spark Apps
> full-fledged Apache Spark applications

## [01 &mdash; GitHub activity Analysis ](./001-github-activity-analysis/)
Analyzing actions on GitHub using the *SparkApplicationTemplate* and *wconf*.

## [02 &mdash; Reading CSV and Writing Parquets](./002-read-csv-write-dataset/)
Reading CSV files using Spark core methods and writing Parquet datasets with different compression formats.

## [03 &mdash; Processing a structured Purchase Log with Spark Core](./003-complimentary-customer-gifts/)
The application loads a structured text file and applies some business rules using Spark Core module. The result of the processing is then written to the local file system as a text file with the same structure.

## [04 &mdash; Count Words using Spark Core](./004-count-words/)
Illustrates how to count the words from file downloaded from the Internet using Spark Core module. By contrast to [03 &mdash; Processing a structured Purchase Log with Spark Core](./003-complimentary-customer-gifts/), the sorting is performed in a materialized `Map` instead that on an *RDD*.