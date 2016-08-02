010-accumulators
================

# Application Specs
+ Create a custom accumulator named `my-accumulator`. Then create an RDD containing integers from `1` to `1_000_000` and create a foreach task that increments the accumulator for each of the items of the RDD. Then query the value of the accumulator after the task has been completed. After that, create another foreach task that queries the value of the accumulator and verify that an exception is raised.

+ Compute the average of the numbers of an RDD using a custom Accumulable that keeps track of the count and the total.  

# Concepts 
Accumulator

# Notes
n/a