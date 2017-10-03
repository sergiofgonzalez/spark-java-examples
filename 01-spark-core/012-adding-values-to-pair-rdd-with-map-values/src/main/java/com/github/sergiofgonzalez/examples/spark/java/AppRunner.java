package com.github.sergiofgonzalez.examples.spark.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.spark.api.java.JavaPairRDD;
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
						.withName("012-adding-values-to-pair-rdd-with-flat-map-values")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(addingValuesApp)
						.submit());		
	}
	
	

	
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> addingValuesApp = (spark, sparkContext) -> {
		JavaPairRDD<Integer, String> wordsByLineNumberRDD = sparkContext.parallelizePairs(Arrays.asList(
				new Tuple2<Integer,String>(1, "This is the first line"),
				new Tuple2<Integer,String>(2, "Hello to Jason Isaacs"),
				new Tuple2<Integer,String>(3, "# This is a comment and should be removed"),
				new Tuple2<Integer,String>(4, "Hello to Idris Elba")
		));
		
		LOGGER.info("Num elements: {}", wordsByLineNumberRDD.count());
		wordsByLineNumberRDD.foreach(line -> LOGGER.info("{}={}", line._1(), line._2()));

		
		wordsByLineNumberRDD = wordsByLineNumberRDD.flatMapValues(line -> {
			List<String> values = new ArrayList<>();
			if (!line.startsWith("#")) {
				values.addAll(Arrays.asList(line.split(" ")));				
			}
			return values;
		});
		
		LOGGER.info("Num elements: {}", wordsByLineNumberRDD.count());
		wordsByLineNumberRDD.foreach(line -> LOGGER.info("{}={}", line._1(), line._2()));		
	};
}
