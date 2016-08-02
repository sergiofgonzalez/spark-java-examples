# e04-join-sandbox
> super simple examples on joins


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

And the products file which contains the products catalog the company sell, one per line, with each line delimited by `#`, wuth the following schema:
```
10#LEGO The Hobbit#7314.55#9
```

with the fields being:
    + the id of the product: 10
    + the name of the product: LEGO The Hobbit
    + the unit price: 7314.55
    + the ????: 9

Obtain:
1. Obtain the total amount of money by product id
+ Obtain the names of products with the total amount of money sold

# Concepts
Illustrates how to work with a `JavaPairRDD` and the basic `JavaPairRDD` functions such as:
+ Creation of RDDs and JavaPairRDDs: `parallelize`, `mapToPair`, 
+ Getting keys and values: `keys`, `mapValues`, `lookup`, `flatMapValues`
+ Actions: `count`, `distinct`, `countByKey`, `reduceByKey`, `foldByKey`, `aggregateByKey`, `union`

# Notes
n/a