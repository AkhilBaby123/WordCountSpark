package com.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * This program accepts 2 arguments as input
 * 1st argument - the input file location
 * 2nd argument - The location to which output should be written
 * You must supply these 2 arguments to run the program
 *
 * The input file is expected to be separated by a space
 *
 * The program counts the number of words in the file and write
 * the count to a location provided by output path
 *
 */
object WordCount extends App{
  if (args.length < 2) {
    println("Missing required arguments..")
    System.exit(0)
  }
  val fileName = args(0)
  val outputFile = args(1)
  println(s"Input File Path - $fileName")
  //Create sparkSession
  val spark = SparkSession.builder().appName("WordCount").master("spark://Akhils-Air:7077").getOrCreate()
  val input = spark.read.textFile(fileName)
  println("Read text file ****** ")
  // Pass the rdd to a function to get the count
  val wordCount = countWords(spark,input)
  // Write the output to a file
  wordCount.saveAsTextFile(outputFile)
  // Function which does the word count
  def countWords(spark:SparkSession,input: Dataset[String])  = {
    import spark.implicits._
    input.flatMap(line => line.split(" ")).map(word => (word,1)).rdd.reduceByKey(_+_,1)
  }

  spark.stop()
}

