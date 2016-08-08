# 003-dataframes-missing-values
> Dealing with nulls and missing values in SparkSQL DSL API

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
1. Clean the input dataframe by dropping all rows containing null or NaN in any of the columns.
2. Clean the input dataframe by dropping all rows containing nulls in all of the columns
3. Clean the input dataframe by dropping rows whose acceptedAnswerId column is null
4. Replace null and NaN values with a constant value 0 for columns viewCount, answerCount and acceptedAnswerId
5. Replace null and NaN values with 0 for viewCount column, 0 for answerCount and -1 for acceptedAnswerId
6. Replace the records with id=1177 and acceptedAnswerId=1177 with 3000

## Concepts
+ Learn the capabilities of na() to handle null and NaN values
+ Learn how to replace specific values in columns

## Notes
n/a