# 007-joining-dataframes
> joining data from DataFrames

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
2. Join the posts and votes DataFrames by the `posts.id` and `votes.postId`.
3. Illustrate the different types of joins with a simple example in which two DataFrames are created with the following structure:
  + DF1: {num, spanish} 
  + DF2: {num, english}
  
 

## Concepts
+ Grouping and aggregating data

## Notes
n/a