# 004-spark-basics-load-multiple-files


## Application Specs
Based on the functionality of `003-spark-basics-filter-udf`, this program performs a filter on the results of the previous program matching the results with those found in the `ghEmployees.txt` file. The only difference being that in this case instead of only one file a bunch of files are considered.

## Concepts
The program illustrates how to load a set of files in a `DataFrame`.

## Notes
It is intended to be used in a local Spark installation.

To download the files used in the example you have to run the following commands:

```bash
$ mkdir -p /tmp/github-archive
$ cd /tmp/github-archive
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
$ gunzip -v *
```
