008-customer-statistics
=======================

# Application Specs
Calculate the following statistics about yesterday's transactions.
All the statistics to be provided by customer:
    + average
    + maximum
    + minimum
    + total price of products bought

Yesterday's transaction file contains all purchases from yesterday, one per line, with each line delimited by `#`, with the following schema:
```
2015-03-30#6:55 AM#51#68#1#9506.21
```
with the fields being:
  + the date of the purchase: 2015-03-30
  + the time of the purchase: 6:55 AM
  + the CustomerID: 51
  + the ProductID: 68
  + the Quantity: 1
  + the total price of the purchase: 9506.21


# Concepts 
Illustrates the use of `combineByKey`.

# Notes
n/a