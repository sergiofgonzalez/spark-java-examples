package org.joolzminer.examples.spark.scala;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.io.Source.fromFile
import java.io.File
import java.nio.file.Paths


object App {
  
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName("GitHub push counter")
      .setMaster("local[*]")
      
    val sparkContext = new SparkContext(config)
    val sqlContext = new SQLContext(sparkContext)
    
    val gitHubLogFilename = "2015-03-01-0.json"
    val inputDir = "./src/main/resources";

    val gitHubLogDataFrame = sqlContext.read.json(Paths.get(inputDir, gitHubLogFilename).toString())
    val pushes = gitHubLogDataFrame.filter("type = 'PushEvent'")  
    
    println("Total Number of Records: " + gitHubLogDataFrame.count());
    println("Total Number of Pushes : " + pushes.count());
    pushes.show(5);    
  }
}
