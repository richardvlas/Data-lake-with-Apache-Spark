# Data Lake with Apache Spark
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We'll be able to test the database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare the results with their expected results.

## Project Description
In this project, we will work with Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. We will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We'll deploy this Spark process on a cluster using AWS.

