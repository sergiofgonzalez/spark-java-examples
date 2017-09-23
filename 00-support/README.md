# 00 &mdash; Support, Common Components, Patterns and Idioms
> Spark and non-Spark support classes


## [01 &mdash; `IntListBuilder`](./001-int-list-builder/)
Support class for building a list of integers.

## [02 &mdash; `Spark Template`](./002-spark-template/)
Template for Spark application that streamlines Spark applications and provides a flexible configuration framework that is accessible from both the master and worker nodes. 

## [03 &mdash; `S3FileUploader`](./003-s3-file-uploader/)
Simple stand-alone application that downloads a list of files from an HTTP location, unzips them and uploads them to S3. The example is based in the most recent AWS SDK which unfortunately cannot be used with Spark when `hadoop-aws` is added as dependency.