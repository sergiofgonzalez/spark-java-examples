001-spark-basics
================

# Application Specs
This program loads a JSON file with the GitHub activity from 2013-05-01 00:00 (as found in `src/main/resources` and performs the count of all the records and the count of all the `PushEvent` records.

# Concepts
Illustrates the very basics of Spark, including the creation of the configuration, the `SparkContext` and `SQLContext` and how to read the contents of a JSON file.
Once the JSON file is loaded, the contents are counted and filtered.

# Notes
It is intended to be used in a local Spark installation.
