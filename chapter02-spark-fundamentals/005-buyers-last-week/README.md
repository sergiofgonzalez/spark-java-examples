005-buyers-last-week
====================

# Application Specs
You have a large file that contains clients' transactions log from the last week. Every time a client made a purchase, the server appended a unique client ID to the end of the log file.
At the end of each day the server added a new line, so we have a nice file structure of one line of comma separated user IDs per day.

The application finds out:
+ How many purchases were recorded last week
+ How many different clients actually bought at least one product during that week.
+ The actual different client IDs that bought at least one product during that week.


# Concepts
Illustrates how to use the `map` and also `first` and `top`.

# Notes
It is intended to be used in a local Spark installation.
