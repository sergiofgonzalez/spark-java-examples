package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
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
						.withName("010-hello-pair-rdd-creation")
						.withPropertiesFile("config/spark-job.conf")
						.withExtraConfigVars(args)
						.doing(pairRddCreationApp)
						.submit());		
	}
	
	

	
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> pairRddCreationApp = (spark, sparkContext) -> {
		
		/* Creating a JavaPairRDD from a materialized list of tuples */
		JavaPairRDD<String, Integer> numbers = sparkContext.parallelizePairs(Arrays.asList(
				new Tuple2<String, Integer>("uno", 1),
				new Tuple2<String, Integer>("dos", 2),
				new Tuple2<String, Integer>("tres", 3),
				new Tuple2<String, Integer>("catorce", 14)));
		
		LOGGER.info("Numbers contain {} tuples", numbers.count());
		
		/* Creating a JavaPairRDD from an RDD using mapToPair */
		JavaRDD<String> actors = sparkContext.parallelize(Arrays.asList(
				"Jason Isaacs", "Riz Ahmed", "Idris Elba"));
		
		JavaPairRDD<String,Integer> actorsLength = actors.mapToPair(actor -> new Tuple2<String,Integer>(actor, actor.length()));
		
		LOGGER.info("ActorsLength contain {} tuples", actorsLength.count());
		
	};

}
