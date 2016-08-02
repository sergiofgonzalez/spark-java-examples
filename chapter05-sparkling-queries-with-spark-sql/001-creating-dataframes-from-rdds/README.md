# 001-creating-dataframes-from-rdds
> creating `JavaPairRDD`s from Java Lists and `JavaRDD`s.

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
0. Create an RDD in which every field of the input data is identified as a string
1. Create an RDD in which every row is an instance of a Post class. The class constructor accepts a String array. Use flatMap().
2. Create an RDD in which every row is an instance of a Post class. The class constructor accepts a String array. Use map().
3. Create a DataFrame from the JavaRDD<Post> and check the the data and the schema is the expected one (according to Post class)
4. Create a DataFrame by explicitly specifying a schema.
5. From the previous DataFrame get the columns of the schema

## Concepts
+ createDataFrame()
+ createStructField(), createStructType(), RowFactory.create()
+ printSchema(), columns(), dtypes()
+ show()


## Notes
n/a