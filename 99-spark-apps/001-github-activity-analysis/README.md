# 001-github-activity-analysis
> Analyzing actions on GitHub using the SparkApplicationTemplate and wconf

## Application Specs
A sample Spark application using *SparkApplicationTemplate* to be able to safely rely on a flexible configuration properties framework that work both from Spark master and executors.

The application in itself performs the following actions:
+ Inspects a the value of a configuration property that tells the application to either download a set of files with information about GitHub activities or consider them already available on S3.
+ Counts the total number of records read
+ Filters the `pushEvent` records and counts them
+ Groups the `pushEvent` records by `actor.login`, and sorts them by count
+ Creates a list of custom names to match, puts them in a *broadcast variable*, a UDF that filters records that belong to one of the names of the list and processed them.


## Concepts
+ Usage of `SparkApplicationTemplate` and `wconf`: encryption, command-line arguments, 
+ Basic usage of *SparkSQL* with JSON data: `filter`, `groupBy`, `orderBy`
+ Broadcast variables
+ Spark UDFs

## Execution Notes
The program is prepared to run from Eclipse (development), in non-clustered Spark installation (using `spark-submit`) and in Spark cluster as well. This is achieved using `wconf` configuration properties and the SparkApplicationTemplate features.

Please keep reading to understand the tests performed and how to run them.


## Manual Tests Performed: Detailed
This section describes the tests performed to verify that both the [SparkApplicationTemplate](../00-support/002-spark-template) and [wconf](https://github.com/sergiofgonzalez/waterfall-config) work as expected.
It also gives a lot of details about how the configuration has been used, and the procedures taken to run the tests.

### Static Configuration
*wconf* allows you to write a couple of files:
+ a `config/commonf.conf` file with configuration details that are not subject of major changes
+ an application-level properties file `config/application.conf` with application level properties, profiles, etc. which allows for a lot of configuration flexibility.

Those files are considered *static* because they are placed under `src/main/resources` and will be packaged in the jar and therefore distributed to all Spark executors when running in a cluster.

However, [wconf](https://github.com/sergiofgonzalez/waterfall-config) and the [SparkApplicationTemplate](../00-support/002-spark-template) allows for several ways to override values in those files:
+ You can override specific values by defining environment variables or Java system properties. However, when running in clusters that solution is prone to error as it is complicated to keep in sync configuration in the machines of the cluster. Fortunately, [SparkApplicationTemplate](../00-support/002-spark-template) comes to the rescue and allows you to override config properties values using command line arguments, and it does that in a way that ensures that overridden values are available in all the cluster nodes.
+ You can provide an external property file (not packaged in the jar) that will be used instead of the one packaged in the jar. Because of the challenges associated to file distribution in the Spark cluster and the fact that the configuration properties must be in sync in all the nodes, the external property file must be placed in a location accessible by all the machines of the cluster (i.e. a shared volume).

#### Common Properties
The common properties are kept in `config/common.conf` and in this case only contain a basic set of properties:
```
spark {
  application_name: 001-github-activity-analysis
  master: "local[*]"
}


wconf_ext_app_properties_paths: [ "/media/shared-spark" ]
```

The most interesting config property is `wconf_ext_app_properties_paths`, which is used to configure a set of paths that will be scanned when *wconf* is bootstrapping.
Note also the `spark.master=local[*]` which would limit the applicability of the solution for non-clustered environments if it wasn't for the ability of *wconf* to allow overriding property values.  

#### Application-Level Properties
The static application-level properties are kept in `config/spark-job.conf`. This file contains a top-level property `wconf_active_profile` used to activate a portion of the file, known as a profile in *wconf* parlance.

In short, you can create sets of configuration properties and give them a name. By setting the value of `wconf_active_profile` to one of these name you will be activating that set, and hiding all of the other sets as if they were not in the file:
```
wconf_active_profile: test_profile

local_dev {
  # config properties for local dev
...
}

test_profile {
  # config properties for testing
...
}

prod_profile {
  # config properties for production
...
}
```

Profiles are a powerful concept, as they allow you to capture the environmental differences in a single file and even reference in one profile values defined in another:
```

profile1 {
  foo: bar
}

profile2 {
  foo: ${profile1.foo}
}
``` 

In the application, we defined a set of profiles associated to the different environment tests that will be run:
+ not-clustered-not-aws: when running from Eclipse IDE and processing the files on the local file system
+ clustered-not-aws: when running in a Spark cluster and processing the files on the local file system
+ not-clustered-aws: when running from Eclipse IDE but using files stored on AWS S3. This allows to test the s3a file system and encryption to protect the keys used to access AWS S3.
+ clustered-aws: when running in a Spark cluster and processing files stored on AWS S3.


An additional profile `ext-clustered-aws` is defined in an external file and will be used to test how the *wconf* and *SparkApplicationTemplate* supports passing an external file for setting the configuration properties for your Spark application.


**Note**
the value for `wconf_active_profile` can be overridden as the rest of configuration properties to dynamically select the profile to use when starting the application. The main consequence is that you **won't** need to rebuild your application to use a different profile from the one statically selected in the application-level property file.


### Running from Eclipse using a statically defined Configuration: Not using encryption
This is the simplest of tests, that demonstrates that the application effectively reads the information stored in `config/common.conf` and `config/spark-job.conf`. The files are downloaded in the local file system and processed locally. As no sensitive details like keys or authentication tokens are used, no encryption features are used.

In order to run the test, simply validate the `spark-job.conf` is selecting `not-clustered-not-aws` as the active profile and run the `AppRunner` as a Java application from your Eclipse IDE.

Note that the output contains a log of *sysouts* that will be used in the subsequent tests to verify that the configuration is properly synced in all the Spark cluster executors.

#### Features Tested
+ [X] Statically configuration correctly loaded through SparkApplicationTemplate
+ [X] Statically selected profiles
+ [X] Spark Master configured as selected
+ [X] Merging of common properties with application-level properties

### Running from Eclipse using a statically defined Configuration: Using encryption
This is a twist on the previous test that demonstrates how to fine-tune Spark through the SparkApplicationTemplate initialization hook, and also that the configuration allows for using the *s3a* protocol. As accessing S3 requires using authentication details, the encryption features of *wconf* are also used. 

In order to run the test, as `spark-job.conf` is selecting `not-clustered-not-aws` as the active profile, you will have to override the value using command line arguments, which can be done by creating a running configuration that uses `-X wconf_active_profile=not-clustered-aws` and run that configuration from your Eclipse IDE.

#### Enabling encryption for sensitive data
[wconf](https://github.com/sergiofgonzalez/waterfall-config) framework provides support for encrypting sensitive pieces of data using JCE symmetric encryption features. That functionality is used to protect AWS keys by setting the information in the profile `not-clustered-aws` as follows:

```
not-clustered-aws {
...
  aws {
    filesUploadedToS3: true
    
    access_key_id: "cipher(<the-result-of-encrypting-the-AWS-S3-access-key-id>)"
    secret_access_key: "cipher(<the-result-of-encrypting-the-AWS-secret-access-key>)"
...
}
```  

Note that the recommended practice is **not** to package the keystore with your application, and that is why the profile is setting:
`wconf_encryption.key_store.path: ./spark-wconf-keystore.jks`.

As reference the command used

The rest of the details for enabling encryption are well documented in [wconf](https://github.com/sergiofgonzalez/waterfall-config).

 
 that demonstrates that the application effectively reads the information stored in `config/common.conf` and `config/spark-job.conf`. The files are downloaded in the local file system and processed locally. As no sensible details like keys or authentication tokens are used, no encryption features are used.

In order to run the test, simply validate the `spark-job.conf` is selecting `not-clustered-not-aws` as the active profile and run the `AppRunner` as a Java application from your Eclipse IDE.

Note that the output contains a log of *sysouts* that will be used in the subsequent tests to verify that the configuration is properly synced in all the Spark cluster executors.

#### Features Tested
+ [X] Dynamic profile selection using command line arguments
+ [X] Initialization hook
+ [X] s3a file system
+ [X] Encryption


### Running on local installation of Spark: running from jar
Although not related to *wconf* or the *SparkApplicationTemplate*, the project is configured with *Maven shade plugin* so that you can generate jar and submit the application to Spark using `spark-submit`.
In this particular test, it is demonstrated that you can run the jar using a Spark installation.

#### Preparing the test
You will have to follow these steps to perform the tests:

1. Run the command `mvn clean package` to generate a JAR that will include the necessary external dependencies.
2. Copy the generated jar to a specific location, such as `/tmp/spark-app`:
```bash
$ mkdir -p /tmp/spark-app
$ cp target/001-github-activity-analysis-1.0.0-SNAPSHOT.jar /tmp/spark-app
```
3. Test the statically defined profile using `spark-submit`:
```bash
$ cd $SPARK_HOME
$ ./bin/spark-submit --master local[*] \
--class com.github.sergiofgonzalez.examples.spark.java.AppRunner \
/tmp/spark-app/001-github-activity-analysis-1.0.0-SNAPSHOT.jar
```
4. Copy the keystore to `/tmp/spark-app` (we will need it to access S3 file system):
```bash
$ cp spark-wconf-keystore.jks /tmp/spark-app/
```
5. Test the `not-clustered-aws` profile:
```bash
$ cd $SPARK_HOME
$ ./bin/spark-submit --master local[*] \
--class com.github.sergiofgonzalez.examples.spark.java.AppRunner \
/tmp/spark-app/001-github-activity-analysis-1.0.0-SNAPSHOT.jar \
--extra-config wconf_active_profile=not-clustered-aws
```
This will fail, because the static configuration of the key store points to a non-valid, location.
6. Test the `not-clustered-aws` profile overriding the key store path:
```
$ cd $SPARK_HOME
$ ./bin/spark-submit --master local[*] \
--class com.github.sergiofgonzalez.examples.spark.java.AppRunner \
/tmp/spark-app/001-github-activity-analysis-1.0.0-SNAPSHOT.jar \
--extra-config wconf_active_profile=not-clustered-aws \
-X wconf_encryption.key_store.path=/tmp/spark-app/spark-wconf-keystore.jks
```


#### Features Tested
+ [X] Running from executable jar
+ [X] Overriding application properties


### Running on a Spark cluster: using statically defined configuration
In this section, we use a Spark cluster to validate that the *SparkApplicationTemplate* and *wconf* work well together even on a Spark cluster. 

**Note**
You can use the standalone cluster spec found in [Apache Spark standalone cluster stack](https://github.com/sergiofgonzalez/docker-book/tree/master/03-sample-containers/e13-spark-standalone-cluster) to run the tests in this section. The stack can be deployed to *Docker Compose* or *Docker Swarm*. The example commands use *Docker compose* and take that infrastructure setup as reference.

#### Preparing the test
In this case, the resources (jar and key store) must be available inside the cluster, and therefore, must be copied to the volume that is visible from the executors.

You will have to follow these steps to perform the tests:

1. Run the command `mvn clean package` to generate a JAR that will include the necessary external dependencies.
2. Copy the generated jar and key store to the shared volume `/tmp/spark-app`:
```bash
$ cp 99-spark-apps/001-github-activity-analysis/target/001-github-activity-analysis-1.0.0-SNAPSHOT.jar ~/Development/docker/docker-book/03-sample-containers/e13-spark-standalone-cluster/shared-volume/
cp 99-spark-apps/001-github-activity-analysis/spark-wconf-keystore.jks ~/Development/docker/docker-book/03-sample-containers/e13-spark-standalone-cluster/shared-volume/
```
3. Start the cluster:
```bash
$ sudo docker-compose up
```
4. Obtain the IP address of the master:
```bash
$ sudo docker-compose exec spark-master hostname -I
172.18.0.2
``` 
5. Test `cluster-aws` profile using `docker-compose exec`:
```bash
$ sudo docker-compose exec spark-master \
/bin/bash -c "./bin/spark-submit --master spark://172.18.0.2:7077 \
--class com.github.sergiofgonzalez.examples.spark.java.AppRunner \
/media/shared-spark/001-github-activity-analysis-1.0.0-SNAPSHOT.jar \
-X wconf_active_profile=clustered-aws"
```

In the previous examples, the validation can be done just by verifying that the application works, as there is not distributed system in place, but in this case, you must also verify that the configuration between the master and workers are in *sync*.

This can be done in two steps:
1. Verifying that the logs from the master execution shows: `wconf =>: dummy=a dummy value set in clustered-aws profile`
2. Connecting to the worker and inspecting the logs:
```bash
$ sudo docker-compose exec spark-worker /bin/bash
# cd work
# ls -lrt
...
drwxr-xr-x 3 root root 4096 Sep 23 21:04 app-20170923210408-0001
drwxr-xr-x 3 root root 4096 Sep 23 21:07 app-20170923210736-0002
# cd app-20170923210736-0002
# ls -la
0
# cd 0
# cat stdout
wconf =>: dummy=a dummy value set in clustered-aws profile
```

#### Features Tested
+ [X] Running on Apache Spark cluster
+ [X] Profile selection in sync between master and worker nodes


### Running on a Spark cluster: using external config file
In this section, we use an external config file to validate that *SparkApplicationTemplate* and *wconf* work well together even when the configuration is not packaged with the jar. 


#### Preparing the test
In this case, we also need the conf file `ext-spark-job.conf` to be copied to the volume that is visible from the executors.

You will have to follow these steps to perform the tests:

1. Run the command `mvn clean package` to generate a JAR that will include the necessary external dependencies.
2. Copy the generated jar and key store to the shared volume `/tmp/spark-app`:
```bash
$ cp 99-spark-apps/001-github-activity-analysis/target/001-github-activity-analysis-1.0.0-SNAPSHOT.jar ~/Development/docker/docker-book/03-sample-containers/e13-spark-standalone-cluster/shared-volume/
cp 99-spark-apps/001-github-activity-analysis/spark-wconf-keystore.jks ~/Development/docker/docker-book/03-sample-containers/e13-spark-standalone-cluster/shared-volume/
cp 99-spark-apps/001-github-activity-analysis/ext-spark-job.conf ~/Development/docker/docker-book/03-sample-containers/e13-spark-standalone-cluster/shared-volume/
```
3. Start the cluster:
```bash
$ sudo docker-compose up
```
4. Obtain the IP address of the master:
```bash
$ sudo docker-compose exec spark-master hostname -I
172.18.0.2
``` 
5. Test `cluster-aws` profile using `docker-compose exec`:
```bash
$ sudo docker-compose exec spark-master \
/bin/bash -c "./bin/spark-submit --master spark://172.18.0.2:7077 \
--class com.github.sergiofgonzalez.examples.spark.java.AppRunner \
/media/shared-spark/001-github-activity-analysis-1.0.0-SNAPSHOT.jar \
-X wconf_active_profile=ext-clustered-aws -X wconf_app_properties=ext-spark-job.conf"
```

Again, the validation in this case can be done just by verifying that the application works and that the configuration between the master and workers are in *sync*.

This can be done in two steps:
1. Verifying that the logs from the master execution shows: `wconf =>: dummy=a dummy value set in ext-clustered-aws profile`
`
2. Connecting to the worker and inspecting the logs:
```bash
$ sudo docker-compose exec spark-worker /bin/bash
# cd work
# ls -lrt
...
drwxr-xr-x 3 root root 4096 Sep 23 21:04 app-20170923210408-0001
drwxr-xr-x 3 root root 4096 Sep 23 21:07 app-20170923210736-0002
# cd app-20170923210736-0002
# ls -la
0
# cd 0
# cat stdout
wconf =>: dummy=a dummy value set in ext-clustered-aws profile
```

#### Features Tested
+ [X] Configuration in-sync even when using external config file


## Caveats and Further Considerations
+ The use of external files rely on the existence of a shared volume among master and workers. There is currently no support for having those files in S3 locations, or HDFS. If you can't rely on a shared volume, package your configuration in the jar.
+ *wconf* takes the information on an external file as the config source with the highest priority. Only a bunch of meta config keys such as the active profile, or the name of the application-level property file should be provided via command-line arguments when using external files. This is subject of being changed in a future implementation of *wconf*. 

