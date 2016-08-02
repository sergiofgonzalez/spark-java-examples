# e03-joins
> joining data from Java Pair RDDs

This is another take at 004-trans-product-joins developed with a pair of fresh eyes.

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
    + the number of products in stock: 9

Obtain:
1. The total amount of money by product id
2. The total amount of money by product name (hint: join with products)
3. The list of products that were not sold
4. The sorted list of transactions with product names, sorted by product name 
5. The average, max, min and total price of products bought per customer (hint: use combineByKey)


# Concepts
Illustrates in a working sample involving a couple of files several JavaRDD and JavaPairRDD transformations and actions:
+ Transformations and actions: reduceByKey, join, mapValues, subtractByKey, cogroup
+ Sorting: sortBy, sortByKey, takeOrdered
+ GenericTransformation: combineByKey

# Notes
n/a