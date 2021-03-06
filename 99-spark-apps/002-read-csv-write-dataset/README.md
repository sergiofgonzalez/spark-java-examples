# Reading CSV and Writing Parquet Datasets
> Reading CSV files and writing datasets with different compression format

## Application Specs
Simple application that reads a configured input CSV file with a known schema and writes its contents in a configured location using Parquet file format using one of the available compression methods.

The application is prepared to handle two different types of input CSV files, and can write to the local file system, S3 (using `s3a://` scheme) or Azure Blob Storage (using `wasbs://`).


## Concepts
+ Using the `SparkApplicationTemplate` for an application
+ Reading CSV files using the new `spark.read().csv()` method 
+ Setting a predefined schema for a dataset using `StructField` and `StructType`
+ Writing Parquet files
+ Setting the compression algorithm for Parquet files
+ Writing to S3 and Azure Blob Storage

## Execution Notes
Adapt the configuration files found under `./src/main/resources` and run the application. You can find a couple of sample jobs in `./src/main/resources/input-data`. There is a tool available for generating files of these types in [e02-streaming-random-file-generator](https://github.com/sergiofgonzalez/nodejs-in-action/tree/master/chapter15-streams/e02-streaming-random-file-generator).

Note that for S3/Azure you will need to own the corresponding access details.