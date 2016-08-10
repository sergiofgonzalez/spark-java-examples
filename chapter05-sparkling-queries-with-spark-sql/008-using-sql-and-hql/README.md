# 008-using-sql-and-hql
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
1. Load a DataFrame with the contents of the `italianVotes.csv` input file, explicitly specifying the schema
2. Register both DataFrames as temporary tables in Spark and perform simple selects on them
3. Register both DataFrames as permanent tables and perform simple selects on them
4. Perform a join between the Posts and Votes tables using SQL
  
 

## Concepts
+ Registering temporary tables
+ Registering permanent tables that survive from execution to execution
+ Using a local Hive Metastore not based on Derby, but on MySQL server (note that the POM is also configured to include the MySQL JDBC driver)

## Notes
n/a