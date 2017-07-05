# 005-hello-sampling

## Application Specs

This program illustrates the `JavaRDD.sample` transformation and the `JavaRDD.takeSample` action that lets you sample elements from an RDD.

## Concepts

The `JavaRDD.sample` method is a transformation that creates a new RDD with random elements from the calling RDD:
```java
JavaRDD.sample(Boolean withReplacement, Double fraction, Long seed);
```

+ `withReplacement` &mdash; determines whether the same element may be sampled multiple times. If set to false, an element, once sampled, will be *removed* and therefore not considered for any subsequent sampling.
+ `fraction` &mdash; if `withReplacement == true` it is the expected number of times an element is going to be sampled. Otherwise (without replacement) is the expected probability of sampling for a given element.
+ `seed` &mdash; the seed for the random-number generation (optional).


The `JavaRDD.takeSample` method is an action (not a transformation), that samples an exact number of elements from an RDD:
```java
JavaRDD.takeSample(Boolean withReplacement, Integer num, Long seed);
```

+ `withReplacement` &mdash; determines whether the same element may be sampled multiple times. If set to false, an element, once sampled, will be *removed* and therefore not considered for any subsequent sampling.
+ `num` &mdash; is the number of elements to sample from the RDD.
+ `seed` &mdash; the seed for the random-number generation (optional).

## Notes

+ The program uses the `IntListBuilder` class from the support package ([001-int-list-builder](../../00-support/001-int-list-builder/).
+ It is intended to be used in a non-clustered Spark installation.