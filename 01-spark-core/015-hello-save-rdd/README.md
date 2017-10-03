# Hello Saving an RDD
> using `saveTextFile` to save an RDD 

## Description
`saveTextFile` can be used to save the contents of an RDD. The elements of the RDD are serialized using `toString` so it is recommended to provide your own encoding and map the contents to a String.

In the example, we illustrate how to save files using several and a single partition using Arrays, lists and a custom encoding. Note that there is no `mode(SaveMode.Overwrite)` available for this operation, so `FileUtils.deleteQuietly` is used to delete the output data before saving.

## Concepts
+ Using `saveAsTextFile` to save the contents of an RDD
+ Using `coalesce(1)` to transform an RDD into a single partitionRDD prior to writing the results
