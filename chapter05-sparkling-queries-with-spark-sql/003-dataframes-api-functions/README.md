# 003-dataframes-api-functions
> Using functions with SparkSQL DSL API

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
1. Find the question that has been active for the largest amount of time
2. Show the body column of the question that has been active for the largest amount of time, replacing the escaped HTML sequences by its corresponding character sequences.
3. Show the average and max score of all the posts and the total number of posts with score.
4. Show the maximum score of all the answers of user's questions and an additional column showing the difference between the current score and this maximum (Hint: use Window functions to partition the dataset by *ownerUserId*).
5. Show for each question id of its owner next and previous question by creation date.
6. Define a Spark UDF that count the number of tags in the tags column and show the number of tags for each of the questions.

## Concepts
+ Using built-in scalar and aggregation functions
+ Using Spark UDFs
+ Using Window functions

## Notes
n/a