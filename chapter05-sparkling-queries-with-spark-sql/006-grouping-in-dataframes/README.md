# 006-grouping-in-dataframes
> grouping data with DataFrames

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
1. Find the number of posts per author (ownerUserId), and show them in descending order
2. Find the number of posts per author (ownerUserId), tags and postTypeId and show them in descending order by ownerUserId
3. Find the last activity date by author (ownerUserId)
4. Create a DataFrame with the last activity date and maximum score by author (ownerUserId)
5. Create a DataFrame with the last activity date and a column that shows whether the max score was greater than 5
6. Create a custom UDAF that computes the average. Test it by creating a DataFrame with a double column and applying the UDAF to it.
7. Calculate the rollup and cube for the authors between 13 and 15:
  a. Create a DataFrame containing only the records for ownerUserId 13, 14 and 15
  b. Count the posts grouped by ownerUserId, tags and postTypeId
  c. Compare the results when instead of the groupBy rollup is used
  d. Compare the results when instead of the groupBy cube is used

## Concepts
+ Grouping and aggregating data

## Notes
n/a