# 003-hello-map

## Application Specs

This program illustrates the `JavaRDD.map` operation that lets you transform RDDs. In the example, a list of integers is squared, and then transformed into strings and reversed.

## Concepts

Resilient Distributed Datasets (RDD) are immutable, so each time you apply a transformation, a new RDD is created.

+ Using `JavaRDD.map(Function<Input,Output> mappingFn)` method to transform the contents of an RDD
  + Transforming the elements of an RDD without changing the type
  + Transforming the types of the elements of an RDD

+ Using `JavaRDD.top(int k)` to obtain the k largest elements from this JavaRDD as defined by the implicit ordering for the type of the *JavaRDD* elements. 

## Notes

+ The program uses the `IntListBuilder` class from the support package ([001-int-list-builder](../../00-support/001-int-list-builder/).
+ It is intended to be used in a non-clustered Spark installation.
