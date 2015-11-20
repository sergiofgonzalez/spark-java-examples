007-sort-with-complex-keys
==========================

# Application Specs
Sort a pair RDD whose key is of type Employee, by its first name and its last name using Java 8 style programming (lambdas).

# Concepts 
Illustrates how to use `sortByKey` in scenarios where the pair RDD key is complex. It also illustrates how to create serializable comparators as the ones required by Spark using lambdas.

In particular the latter point is base on using a casting with the intersection operator `&`. 

# Notes
n/a