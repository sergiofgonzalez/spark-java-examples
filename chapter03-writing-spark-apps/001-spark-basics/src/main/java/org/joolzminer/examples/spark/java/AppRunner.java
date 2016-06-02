package org.joolzminer.examples.spark.java;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("001-spark-basics")
								.setMaster("local[*]");

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {
			SQLContext sqlContext = new SQLContext(sparkContext);
			
			String githubLogFilename = "2015-03-01-0.json";
			String inputDir = "./src/main/resources";
			
			DataFrame githubLogDataFrame = sqlContext.read().json(Paths.get(inputDir, githubLogFilename).toString());
			DataFrame pushOperationsDataFrame = githubLogDataFrame.filter("type = 'PushEvent'");
						
			long totalNumberOfRecords = githubLogDataFrame.count();
			long totalNumberOfPushes = pushOperationsDataFrame.count();
			
			/* showing first 5 records in tabular form */
			pushOperationsDataFrame.show(5);
			
			printSeparator();
			
			System.out.println("-- SCHEMA --");
			pushOperationsDataFrame.printSchema();
			
			System.out.println("SUMMARY:");
			System.out.println(String.format("Total Number of records in (%s): %d", githubLogFilename, totalNumberOfRecords));
			System.out.println(String.format("Total Number of pushes  in (%s): %d", githubLogFilename, totalNumberOfPushes));
			
		}
		
	}
	
	private static final void printSeparator() {
		System.out.println("=================================================================");
	}
}
