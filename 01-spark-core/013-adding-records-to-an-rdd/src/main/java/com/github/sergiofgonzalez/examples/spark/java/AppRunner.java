package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("013-adding-records-to-an-rdd")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(addingRecordsApp)
						.submit());		
	}
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> addingRecordsApp = (spark, sparkContext) -> {
		JavaRDD<String> linesRDD = sparkContext.parallelize(Arrays.asList(
				"Hello to Jason Isaacs!",
				"Hello to Idris Elba!"
		));
		
		LOGGER.info("Num elements: {}", linesRDD.count());
		linesRDD.foreach(line -> LOGGER.info("{}", line));
		
		
		/* we can add arbitrary materialized records with union and parallelize */
		List<String> arbitraryLines = Arrays.asList("Hello, Foo!", "Hello, Bar!");
		
		linesRDD = linesRDD.union(sparkContext.parallelize(arbitraryLines));
		
		LOGGER.info("Num elements: {}", linesRDD.count());
		linesRDD.foreach(line -> LOGGER.info("{}", line));
		
		/* or simply we can use union with RDDs */
		JavaRDD<String> linesMetadataRDD = linesRDD.map(line -> String.format("The line '%s' is %d char(s) long", line, line.length()));
		linesRDD = linesRDD.union(linesMetadataRDD);
		
		LOGGER.info("Num elements: {}", linesRDD.count());
		linesRDD.foreach(line -> LOGGER.info("{}", line));
		
		/* Now we create a JavaPairRDD, to demonstrate the same things can be done with them */
		JavaPairRDD<String, Integer> linesWithLengthRDD = linesRDD.mapToPair(line -> new Tuple2<>(line, line.length()));

		LOGGER.info("Num elements: {}", linesWithLengthRDD.count());
		linesWithLengthRDD.foreach(tuple2 -> LOGGER.info("{}; length={}", tuple2._1(), tuple2._2()));
		
		List<Tuple2<String,Integer>> arbitraryTuples = Arrays.asList(new Tuple2<>("hello", 5));
		linesWithLengthRDD = linesWithLengthRDD.union(sparkContext.parallelizePairs(arbitraryTuples));
		
		LOGGER.info("Num elements: {}", linesWithLengthRDD.count());
		linesWithLengthRDD.foreach(tuple2 -> LOGGER.info("{}; length={}", tuple2._1(), tuple2._2()));
	};
}
