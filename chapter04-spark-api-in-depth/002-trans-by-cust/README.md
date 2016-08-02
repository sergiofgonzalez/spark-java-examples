# 002-trans-by-cust
> working with JavaPairRDDs

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
+ Find the client who placed the greatest number of purchases
+ Display the purchases placed by client who place the greatest number of purchases
+ Apply 5% discount to the purchases with two or more items with ProductID=25
+ Add a complimentary productID = 70 to customers who bought 5 or more productID = 81
+ Find the customer who spent the most overall
+ Include a complementary productID 4 to the client who placed more purchases and productID 63 to the client who spent the most
+ Find the products each customer purchased
+ Save the final state of the transaction file using the same format as the original one

# Concepts
Illustrates how to work with a `JavaPairRDD` and the basic `JavaPairRDD` functions such as:
+ Creation of RDDs and JavaPairRDDs: `parallelize`, `mapToPair`, 
+ Getting keys and values: `keys`, `mapValues`, `lookup`, `flatMapValues`
+ Actions: `count`, `distinct`, `countByKey`, `reduceByKey`, `foldByKey`, `aggregateByKey`, `union`

# Notes
n/a