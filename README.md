# elonmuskTweets
 
# Spark RDD APIs
This project is focused on building a Spark application using Scala to extract statistics from a large number of tweets posted by Elon Musk stored in a CSV file. The application receives a list of keywords as input and calculates the following statistics:

## Getting Started
To get started with the project, you will need to have Spark and Scala installed on your machine. Additionally, you will need to import the necessary libraries and the csv file containing the tweets.

## Prerequisites
- [Apache Spark](https://spark.apache.org/)
- [Scala](https://www.scala-lang.org/)

## Installing
To install Spark and Scala, please follow the instructions provided on the respective websites.

## Running the Application
The application is run by inputting the keywords in a comma-separated list as a command-line argument.
For example, to run the application with keywords "Tesla" and "SpaceX", the command to run the application is as follows:

`Tesla, SpaceX`

The application will output the following statistics for each keyword entered:
- Day-wise distribution of keywords over time, i.e., the number of times each keyword is mentioned every day. For example, the output will be in the format of: (k1, 2-3-2021, 34), (k1, 3-3-2021, 14)
- The percentage of tweets that have at least one of these input keywords.
- The percentage of tweets that have at most two of the input keywords.
- The average and standard deviation of the length of tweets. Note that the length of tweet refers to the number of words in that tweet.
- The output will be in the form of a tabular format, where each keyword will have its own row and the statistics will be displayed in columns.

## Note
The application uses Spark RDD APIs to process the data and extract the necessary statistics. It's a hands-on project to master Scala and Spark for data processing and analysis.
