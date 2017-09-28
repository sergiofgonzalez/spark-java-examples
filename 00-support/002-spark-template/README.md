# Spark Template
> A template that streamlines Spark processing and provides a flexible configuration framework for application level properties

## withSparkDo &mdash; a Spark application template

Use `withSparkDo` whenever you want to perform custom Spark processing using Spark core and Spark SQL modules in a managed way that provide the following benefits:
+ custom Spark processing will be provided with ready to use `SparkSession` and `JavaSparkContext` objects
+ a simple API for reading configuration properties that is safe to use both from the master and worker nodes
+ automatic exception handling when an error is raised while processing
+ basic execution timers whose information will be available in the console 


### Simple Usage
```java
    withSparkDo(cfg -> cfg
            .withName("GithubDayAnalysis")
            .withPropertiesFile("config/spark-job.conf")
            .withExtraConfigVars(args)
            .doing(githubAnalysisApp)
            .afterSparkInitializedDo(awsAwareInitializationHook)
            .submit());   
```

`withSparkDo` you're given a configuration object with a bunch of builder-like methods you can use to tailor how your application will be executed:

| method                | description |
|-----------------------|-------------|
| `withName` | configure the Spark application name |
| `withPropertiesFile` | select the application-level properties file (if any) |
| `withExtraConfigVars` | configure additional command line args (if any) to override configuration properties |
| `doing` |: pass the actual Spark application to execute as a `BiConsumer<SparkSession,JavaSparkContextx>` |
| `afterSparkInitializedDo`| configure the initialization actions (if any) as a `BiConsumer<SparkSession,JavaSparkContextx>` |
| `afterProcessingComplete`| schedule finalization actions (if any), also as a `BiConsumer<SparkSession,JavaSparkContextx>` |

## Setting the application name when using the Spark Template

Use the `withName` method to specify the require application name with which your application will be identified. The method expects an `String` argument.

## Setting up the Configuration Properties for the application

The Spark Template bases all of its configuration capabilities on [wconf](https://github.com/sergiofgonzalez/waterfall-config). This framework allows you to specify most of your configuration properties in a couple of files:
+ a `config/commonf.conf` file with configuration details that are not subject of major changes, or that will be shared among several Spark applications.
+ an application-level properties with properties that only make sense for the application that is being executed.

Those files are considered *static* because they are placed under `src/main/resources` and will be packaged in the jar and therefore distributed to all Spark executors when running in a cluster.

However, the SparkApplicationTemplate allows provides a way to to override values in those files by either:
+ passing an array of command line arguments prefixed with `-X` or `--extra-config`.
+ providing an external property file (not packaged in the jar) that will be used instead of the one packaged in the jar. 

**Notes**
+ Because of the challenges associated to file distribution in the Spark cluster and the fact that the configuration properties must be in sync in all the nodes, the external property file must be placed in a location accessible by all the machines of the cluster (i.e. a shared volume).
+ The overriding system in place for configuration properties lets you even select a different file for the application-level configuration properties than the one passed in the `.withPropertiesFile()` invocation. As a direct consequence you won't need to rebuild your application to select a different file. 

All capabilities provided by [wconf](https://github.com/sergiofgonzalez/waterfall-config) can be used by the applications using the *Spark Application Template*. Those that are of special interest for Spark application development include:
+ [encryption](https://github.com/sergiofgonzalez/waterfall-config#encrypted-properties) of sensitive configuration properties
+ [profiles](https://github.com/sergiofgonzalez/waterfall-config#using-profiles) in application-level property files
+ [external application files](https://github.com/sergiofgonzalez/waterfall-config#customizing-application-configuration-files)

## Selecting the default application-level properties file

Use `withPropertiesFile` to select the *default* application-level properties file that will be used unless overridden by additional configuration properties (when using `withExtraConfigVars`). The method expects a `String` representing a relative path.

You should typically use `withPropertiesFile` to identify a file within your *jar* that contains the set of variables that your application needs (such as multi-language messages, timestamp formats, input file paths, etc.). 

## Specifying processing actions when using the Spark Template

Use the `doing()` method to specify a function that will receive a `SparkSession` and a `JavaSparkContext`to perform the custom actions of your application.
That function, specifically a `BiConsumer<SparkSession,JavaSparkContext>` that will be activated as soon as:
1. The `SparkSession` and `JavaSparkContext` have been constructed and are ready to be used.
2. The `wconf` framework has been correctly bootstrapped with the given parameterization, so that is safe to use from the master and worker nodes.
3. The *custom initialization actions* you provided via `afterSparkInitializedDo` have been completed. 

The actions to perform in the function you pass to the `doing` method are not constrained in any way.

## Specifying additional configuration properties (or overriding existing ones)

Use `withExtraConfigVars` to pass additional configuration properties to the Spark application template. Those properties can either be used to override the ones specified in the static `config/common.conf` file and the application-level properties file, or to define additional ones.

The method expects an array of `String` objects in which each of the elements has the format `-X config_property=property_value` or `--extra-config config_property=property_value`. Any other element in the array will be silently ignored.

Note that the properties defined using `withExtraConfigVars` will be available both on workers and masters.

## Executing initialization actions

Use `afterSparkInitializedDo` to specify a function that will be executed before the one configured using `doing()`. That actions are meant to be non-functionality related initialization tasks, although technically speaking, that is not technically enforced. As an example, configuring the compression algorithm for the *Parquet* files, or setting up the *s3a* file system when working on Amazon Web Services make a good example of initialization actions.

The method expects a `BiConsumer<SparkSession,JavaSparkSession>` with the actions to perform. Note that `wconf()` will be readily available to use within your initialization actions.

## Executing cleanup actions

Use `afterProcessingComplete` to specify a function that will be executed right after the one configured using `doing()`. As in the case of the initialization hook, the actions for `afterProcessingComplete` are meant to be non-functionality related. As an example, cleaning up intermediate files or makes a good example of a cleanup action you can configure.

The method expects a `BiConsumer<SparkSession,JavaSparkSession>` with the actions to perform. Note that `wconf()` will be readily available to use within your initialization actions.

## Executing in a cluster and the `spark.master` configuration property

The Spark Application Template is readily available for the different environments on which your application may run:
+ As a Java application in your IDE
+ In a non-clustered installation of Spark using `spark-submit --master local[*] ...`
+ In a standalone Spark cluster with master and worker nodes using `spark-submit --master spark://w.x.y.z:7077 ...`
+ In a *yarn* Spark cluster using `spark-submit --master yarn ...`
+ ...

While bootstrapping, the Spark application template will look for a specific configuration property named `spark.master` and use it to configure the `SparkConf` object with which the `SparkSession` is created. However, if you use `spark-submit --master` the value you pass will take precedence over the one configured in `spark.master`.

## Examples

You can find a simple example of Spark application template usage in [Reading CSV and Writing Parquets](../../99-spark-apps/002-read-csv-write-dataset/) and a more contrived example in [GitHub activity Analysis ](../../99-spark-apps/001-github-activity-analysis/)
