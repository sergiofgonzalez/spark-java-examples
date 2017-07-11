# 001-int-list-builder
> Simple DSL to build a list of integers 

## Application Specs
This support class is a convenience class to build a list of integers using a simple DSL that resembles Scala to/by syntax.

The class provides the following methods:
+ `from` &mdash; configures the starting value for the generated list (inclusive)
+ `to` &mdash; configures the end value for the generated list (inclusive)
+ `by` &mdash; optional; configures the step

The class supports both ascending and descending lists

Example 1: Generate list of numbers from 0 to 10 (inclusive)
```java
 List<Integer> nums = new IntListBuilder()
                            .from(0)
                            .to(10)
                            .build();
```

Example 2: Generate list of even numbers from 0 to 10 (inclusive)
```java
 List<Integer> nums = new IntListBuilder()
                            .from(0)
                            .to(10)
                            .by(2)
                            .build();
```

Example 3: Generate list of numbers from 5 to 3 (inclusive)
```java
 List<Integer> nums = new IntListBuilder()
                            .from(5)
                            .to(3)
                            .build();
```


## Notes
You can use this class from the examples by adding the following dependency to your POM:
```xml
    <dependency>
      <groupId>org.joolzminer</groupId>
      <artifactId>001-int-list-builder</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
```
