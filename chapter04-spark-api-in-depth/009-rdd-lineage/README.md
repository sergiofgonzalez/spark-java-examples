009-rdd-lineage
===============

# Application Specs
Create an application that performs the following actions:
+ Create a list of 500 random numbers from 0 to 9 and parallelize it across 5 partitions
+ Create from it a pair RDD whose tuple is (n, n * n)
+ Group the results by key using the formula (k, (sum(v)), that is, reduce by key summing up all the values
+ Convert each of the elements to a string representing the item: (k, v) -> (K={{k}}, V={{v}}). This mapping should not cause shuffling. 

# Concepts 
Illustrates the use of `toDebugString` to display the RDD lineage of a Spark job. The program creates a list of numbers and apply several transformations that work inside and across partitions to obtain the two types of dependencies (narrow and shuffle).

# Notes
n/a