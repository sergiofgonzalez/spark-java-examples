# Adding values to a JavaPairRDD using flatMapValues transformation
> Using `flatMapValues` to change the number of elements associated to a given key when processing a `JavaPairRDD`

## Description
Illustrates how you can change the number of values associated to a given key when working with a `JavaPairRDD` by using the `flatMapValues` transformation.
When using `flatMapValues` you must return a list of elements that will be bound to key being processed, which can be the same number of elements with some modifications, a smaller or larger list of elements or even an empty list.

In the example, we create a *PairRDD* with the structure (line_number, line) and transform it into a (line_number, line_words), taking into account that lines prepended by *#* will be ignored.


## Concepts
+ Using `JavaSparkContext.textFile` to load a file into a *flat* (i.e. without structure `JavaRDD`).
+ Using `JavaPairRDD.flatMapValues` to change the number of elements associated to a given key
