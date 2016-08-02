# e05-accumulables-sandbox
> Ballpark environment for testing Spark accumulables

# Application Specs

1. Using an RDD consisting of a single integer field containing the numbers from 1 to 1_000_000
  1. Calculate sum of all entries
  2. Calculate the average of all entries

2. Using the sample-data.csv file which features the following structure:
```
1stAVE,2016-06-01 23:45,0.176615,0.102555,0,12.4619,0,15,979
```
with the fields being:
  + the site where a given measure has been taken: 1stAVE
  + the time when the measure was taken: 2016-06-01 23:45
  + a series of values for the parameters being measured: 0.176615,0.102555,0,12.4619,0,15,979

There are neither nulls, nor missing values 
  
  1. Calculate sum of all parameters (columns 2 thru 7)
  2. Calculate average of all parameters (columns 2 thru 7)
  3. Calculate the sum of all parameters (columns 2 thru 7) grouped by the first column 


# Concepts 
Using accumulables leveraging the framework i designed for Java 8.

# Notes
n/a