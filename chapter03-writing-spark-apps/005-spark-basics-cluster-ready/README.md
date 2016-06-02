# 005-spark-basics-cluster-ready


## Application Specs
Based on the functionality of `004-spark-basics-filter-udf`, this program performs a filter on the results of the previous program matching the results with those found in the `ghEmployees.txt` file. In this example the results are written in JSON format to a given directory

## Concepts
The program illustrates the definition of user defined functions `UDF` in filtering operations and how to broadcast variables to reduce the network traffic in cluster installations.

## Notes
To download the files used in the example you have to run the following commands:

```bash
$ mkdir -p /tmp/github-archive
$ cd /tmp/github-archive
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
$ gunzip -v *
```
or use `e01-spring-async-file-downloader` to download the files to your local file system.

A sample file with employees can be found in `src/main/resources/ghEmployees.txt`.

The application is prepared to be run locally or submitted to a cluster using `spark-submit`. The application requires the following arguments:

+ arg[0] : The directory that holds another directory named `github-archive` with all the github log files. The default is `/tmp`.
+ arg[1] : The path to the employees file. The default is `./src/main/resources/ghEmployees.txt`.
+ arg[2] : The path to the directory that will hold the output results. The default is `/tmp/out`.


You can create a runnable jar that can be submitted to execution using `spark-submit` using: 
```bash
$ mvn clean package 
```

You can either run it from Eclipse or use the following spark-submit command to execute it. The physiognomy of the spark-submit command is as follows:
```bash
$ cd $SPARK_HOME
$ ./bin/spark-submit --class {{class containing the main() method of your project}} \
   --master {{it's either local[*] or the location of a cluster such as: spark://ip-172-31-3-178:7077}} \
   --name "{{The name of the job}}" \
   /path/to/runnable/jar \
   arg0 arg1 ... argN   
```

For example, to execute this program in a local cluster, assuming the runnable jar is in the target directory you would use:

```bash
$ cd $SPARK_HOME
$ ./bin/spark-submit --class org.joolzminer.examples.spark.java.AppRunner \
  --master local[*] \
  --name "Daily Github Push Counter" \
  /home/ubuntu/Development/git-repos/spark-in-action-repo/spark-java-examples-parent/chapter03-writing-spark-apps/005-spark-basics-cluster-ready/target/005-spark-basics-cluster-ready-1.0.0-SNAPSHOT-executable.jar \
  /tmp /home/ubuntu/Development/git-repos/spark-in-action-repo/spark-java-examples-parent/chapter03-writing-spark-apps/005-spark-basics-cluster-ready/src/main/resources/ghEmployees.txt /tmp/out
```
Note that in the previous example we will be:
+ storing the github-archive on `/tmp`
+ reading the employees files from `./src/main/resources/ghEmployees.txt`
+ writing the results on `/tmp/out`

**NOTE**
The execution will no longer fail if `/tmp/out` already exists, as `SaveMode.Overwrite` is used.
