# 006-prize-giveaway


## Application Specs
You have a large file that contains clients' transactions log from the last week. Every time a client made a purchase, the server appended a unique client ID to the end of the log file.
At the end of each day the server added a new line, so we have a nice file structure of one line of comma separated user IDs per day.

The application returns a sample that contains 7% of random client IDs from the same log, and also another sampling is performed that returns exactly 5 elements without replacement from the list of unique client IDs.


## Concepts
Illustrates how to use the `sample` and `takeSample`.
It also illustrate how to use `min` and `max` which must be passed a Serializable Comparator. In the example, it is demonstrated how you can either use:
```java
    uniqueIds.min(intSerializableComparator()));
    uniqueIds.max((Comparator<Integer> & Serializable) Integer::compare));         

...
    
    public static Comparator<Integer> intSerializableComparator() {
        return (Comparator<Integer> & Serializable) Integer::compare;
    }
```


## Notes
It is intended to be used in a local Spark installation.
