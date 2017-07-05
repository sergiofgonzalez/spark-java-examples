# 004-hello-flatmap

## Application Specs

This program illustrates the `JavaRDD.flatmap` operation that lets you flatten an RDDs whose elements are itself collections.
For example, for an RDD like:
```
RDD = [
[ 1, 2, 3 ],
[ 4, 5, 6 ],
[ 7, 8, 9 ]
]

flatMap => [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

In the example, we load a simple CSV containing the ids of customers that placed a purchase during a day, and we obtain the distinct list of buyers in the file by using `flatMap` and `distinct`.


## Concepts
+ `JavaRDD.flatMap(Function<Input,Iterator<Output>> flatteningFn)` &mdash; flattens and transforms RDDs whose elements are collections
+ `JavaRDD.count` &mdash; returns the number of elements of an RDD
+ `JavaRDD.distinct` &mdash; returns an RDD with the distinct elements of the source RDD 
+ `JavaRDD.collect` &mdash; materializes an RDD into an equivalent Java object

## Notes

+ `flatMap` flattening function must return an Iterator. The easiest way to do that in Java is to use the `ArrayList.iterator` method.
+ `flatMap` also allows for filtering while flattening, you just have to remove the element you want to filter out from the ArrayList you're returning in the flattening function.