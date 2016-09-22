# 001-dstream-basics
> xxx

## Application Specs
The application must be prepared to ingest as input data, comma separated filed written in the `./src/main/resources/streaming-dir`. Mini-batches should be run every 5 seconds.
+ **Order timestamp** mdash; in the format yyyy-mm-dd hh:MM:ss
+ **Order ID** mdash; a serially incrementing integer
+ **Client ID** mdash; a random integer in the range [1, 100]
+ **Stock Symbol** mdash; a random string stock symbol
+ **Amount** mdash; random number from 1 to 1000 indicating the number of stocks bought/sold
+ **Price** mdash; a random number from 1 to 100 representing the price at which to buy or sell
+ **Action** mdash; a character {B, S} identifying whether it is a buy or a sell

Obtain:
0. Establish the streamin context 


## Concepts
+ createDataFrame()



## Notes
n/a