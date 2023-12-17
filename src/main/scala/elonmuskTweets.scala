package com.nidisoft.scala

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

object elonmuskTweets {

  def main(args: Array[String]): Unit = {
    // Configure log4j
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // Initialize Spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("elonmuskTweets-example")
      .getOrCreate()

    import spark.implicits._

    // Read data from CSV
    val elonmuskData = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("src/main/elonmusk_tweets.csv")

    // Display top 10 rows
    println("Original DataFrame : ")
    elonmuskData.show(10)

    // Input keywords
    println("PLEASE enter a list of keywords separated by a comma (e.g., keyword1,keyword2)\n")
    println("INPUT: ")
    val userInput = scala.io.StdIn.readLine()
    val keyWords: List[String] = userInput.split(",").map(_.trim).toList

    // Select Date and Text
    val elonmuskDataUpdate = elonmuskData.select(split(col("created_at"), " ").getItem(0).as("Date"), 'text)
    println("Selecting date and text from the original dataframe")
    elonmuskDataUpdate.show(10)

    println("*************************************************************************************************")

    // UDF for counting word occurrences
    def CountNumOfWord(word: String) = udf((str: String) =>
      word.r.findAllIn(str).size
    )

    // Function for adding a new column with word occurrences
    def NumberWithWord(W: String)(df2: DataFrame): DataFrame = {
      df2.withColumn(colName = W, CountNumOfWord(W)($"text"))
    }

    // Process each keyword
    keyWords.foreach(word => {
      // Calculate word occurrences
      val newDF = elonmuskDataUpdate.transform(NumberWithWord(word))
      println(s"$word occurrences in each tweet:")
      newDF.show(10)

      // Group by Date and calculate the sum of word occurrences
      val newDF2 = newDF.groupBy("Date").sum(word)
      println(s"DataFrame after grouping by date and finding the sum of $word occurrences:")
      newDF2.show(10)

      // Print data as tuples
      val newRDD = newDF2.rdd
      newRDD
        .map(line => (word, line(0), line(1)))
        .foreach(println(_))
    })

    println("*************************************************************************************************")

    // UDF to create an array of integers indicating keyword presence
    def countPercentage = udf((s: String) => {
      keyWords.map(i => if (i.r.findAllIn(s).nonEmpty) 1 else 0)
    })

    // Add a new column with the list from countPercentage function
    val resDF = elonmuskDataUpdate.withColumn("count", countPercentage(col("text")))

    // UDF to find the sum of the list
    def countSummation = udf((x: mutable.WrappedArray[Int]) => {
      x.sum
    })

    // Add a new column with countSummation function
    val DFwithsummation = resDF.withColumn("summation", countSummation(col("count")))

    // Show the DataFrame
    println("DataFrame after adding an array and sum of the list:")
    DFwithsummation.show(3, truncate = false)

    // Calculate and print the percentage of tweets with at least one keyword
    println("Percentage of tweets with at least one of these input keywords: " , (DFwithsummation.filter('summation > 0).count().toFloat / DFwithsummation.count().toFloat) * 100)

    // Calculate and print the percentage of tweets with at most two keywords
    println("Percentage of tweets with at most two of the input keywords: " , (DFwithsummation.filter('summation === 2).count().toFloat / DFwithsummation.count().toFloat) * 100)

    // Calculate and print the average and standard deviation of the tweet length
    val dfCounter = elonmuskDataUpdate.withColumn("size", size(split($"text", " ")))
    println("DataFrame with the number of words in each Tweet:")
    dfCounter.show(10)
    println("Average and standard deviation of the length of tweets:")
    dfCounter.select(avg('size)).show()
    dfCounter.agg(stddev_pop('size)).show()

    // Exit the application
    System.exit(0)
  }
}
