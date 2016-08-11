# 009-saving-and-loading-dataframes
> using SQL and HiveQL (HQL) in Spark

## Application Specs
Using the `italianPosts.csv` as the input data source, which features the following structure of fields delimited by **~**:
+ **commentCount** &mdash; number of comments related to the question or answer
+ **lastActivityDate** &mdash; date and time of last modification
+ **ownerUserId** &mdash; user ID of the owner
+ **body** &mdash; textual contents of the question or answer
+ **score** &mdash; total score of the question or answer based on upvotes and downvotes
+ **creationDate** &mdash; date and time of creation
+ **viewCount** &mdash; the view count
+ **title** &mdash; the title of the question
+ **tags** &mdash; a set of tags the question has been marked with
+ **answerCount** &mdash; number of related answers
+ **acceptedAnswerId** &mdash; if a question, contains the ID of its accepted answer
+ **postTypeId** &mdash; type of the post: 1 for questions, 2 for answers
+ **id** &mdash; post's unique ID

And the `italianVotes.csv` which features the following structure of fields delimited by **~**:
+ **id** &mdash; the id of the vote
+ **postId** &mdash; the id of the post the vote applies to
+ **voteTypeId** &mdash; the type id of the vote
+ **creationDate** &mdash; the date on which the vote was submitted


Obtain:
0. Load a DataFrame with the contents of the `italianPosts.csv` input file, explicitly specifying the schema
1. Save the DataFrame as as JSON, ORC and Parquet file without interacting with the Hive metastore
2. Save the DataFrame as a table in MySQL using JDBC:
  + Create an empty database mysparkdfs in MySQL
  + Use the `SaveMode.Overwrite` and check that the file structure and data is created
3. Save the DataFrame in an existing MySQL table using JDBC:
  + Use the `SaveMode.Append` and check that the table has twice as many rows.
4. Save the DataFrame as JSON, ORC, and Parquet file and register it as a table in the Hive Metastore.
5. From the tables registered in the metastore create new dataframes 
6. Delete the recently created tables from the Hive metastore
7. Load data from relational db using JDBC
8. Refresh the data in the db and check data is refreshed
  + Set a breakpoint and modify some of the table's data
9. Load data from JSON file, and check that the schema is inferred (note: timestamps are declared as strings)
10. Load data from Parquet file, and check that the schema is perfectly inferred

## Concepts
+ Registering temporary tables
+ Registering permanent tables that survive from execution to execution
+ Using a local Hive Metastore not based on Derby, but on MySQL server (note that the POM is also configured to include the MySQL JDBC driver)

## Notes
n/a