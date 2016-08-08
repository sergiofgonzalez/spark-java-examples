# 005-dataframes-to-rdds
> Converting DataFrames back to RDDs

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
1. Convert the DataFrame back to a JavaRDD<Row>
2. Convert the DataFrame to a JavaRDD<Row> and then use map() to replace `&lt;` and `&gt;` escape sequences in the body column by the equivalent characters. Convert the result back to a DataFrame.
3. replace `&lt;` and `&gt;` escape sequences in the body using SQL functions (Hint: use a UDF).


## Concepts
+ Converting DataFrames to JavaRDD<Row>

## Notes
n/a