002-trans-by-cust
=================

# Application Specs
Given yesterday's transaction file which contains all purchases from yesterday, one per line, with each line delimited by `#`, with the following schema:
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

Obtain:
+ A pair RDD in which every element is (customerID, transaction Fields).
+ Find the number of distinct buyers
+ Find the number of purchases made by each client
+ Find the client who bought the greatest number of products

+ Include a complementary productID 4 to the client who bought the greatest number of products
+ Include a complementary productID 63 to the client who spent the most

# Concepts
Illustrates how to work with a `JavaPairRDD` and the basic `JavaPairRDD` functions such as:
+ Creation of pair RDD: `mapToPair`, 
+ Getting keys and values: `keys`
+ Actions: `count`, `distinct`, `countByKey`

# Notes
n/a