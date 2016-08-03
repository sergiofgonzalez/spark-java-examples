# 002-dataframes-api-basics
> SparkSQL DSL methods for selecting, filtering and sorting

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

Obtain:
0. Load a DataFrame with the contents of the input file, explicitly specifying the schema
1. Select the columns id and body passing the column names
2. Select the columns id and body passing Column objects
3. Select all the columns from the DataFrame except the body (hint: use drop)
4. Filter all records that contain the wor *Italiano* in its body, print them and count them
5. Filter all the questions that do not have an accepted answer
6. Rename column *ownerUserId* as *owner*
7. Add a new column consisting in the metric `ratio=viewCount/score` for questions. Print the records whose metric is less than 35.
8. Sort and show the 10 most recently modified questions (hint: use limit and desc)

## Concepts
+ Selecting columns from a DataFrame: select, DataFrame.column 
+ Filtering records from a DataFrame: filter, where
+ Adding, removing and renaming columns from existing DataFrames: withColumn, drop, withColumnRenamed
+ Sorting DataFrames by given columns: sortBy, orderBy
+ Simple column expressions

## Notes
n/a