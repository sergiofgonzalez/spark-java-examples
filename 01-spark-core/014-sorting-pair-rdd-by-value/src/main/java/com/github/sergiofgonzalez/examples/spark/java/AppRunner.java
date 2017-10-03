package com.github.sergiofgonzalez.examples.spark.java;

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
import static java.util.stream.Collectors.*;

import java.util.ArrayList;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("014-sorting-pair-rdd-by-value")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(sortingByValueApp)
						.submit());		
	}
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> sortingByValueApp = (spark, sparkContext) -> {
		
		/* Sorting PairRDD by Value is not supported out of the box */
		JavaPairRDD<String,Integer> lineAndLengthRDD = sparkContext
															.parallelize(Arrays.asList("hello", "to", "Jason", "Isaacs"))
															.mapToPair(str -> new Tuple2<>(str, str.length()));
		LOGGER.info("RDD length: {}", lineAndLengthRDD.count());
		lineAndLengthRDD.foreach(tuple -> LOGGER.info("({}, {})", tuple._1(), tuple._2()));
				
		/* But you can swap keys and values and use sortByKey */
		List<Tuple2<String, Integer>> sortedByLength = lineAndLengthRDD
															.mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
															.sortByKey()
															.collect()
															.stream()
															.map(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
															.collect(toList());
		
		LOGGER.info("List length: {}", sortedByLength.size());
		sortedByLength.stream().forEach(tuple -> LOGGER.info("({}, {})", tuple._1(), tuple._2()));
		
		/* the same technique can be used for more complex value records */
		JavaPairRDD<String,List<String>> actorGreetingsPairRDD = sparkContext.parallelizePairs(Arrays.asList(
				                                  /* name,          preferred greeting,      birth year */
				new Tuple2<>("Jason", Arrays.asList("Jason Isaacs", "Hello to Jason Isaacs!", "1985")),
				new Tuple2<>("Idris", Arrays.asList("Idris Elba", "Yo Idris!", "1972")),
				new Tuple2<>("Riz", Arrays.asList("Riz Ahmed Sr.", "Hey Riz!", "1946"))));
		
		LOGGER.info("List length: {}", actorGreetingsPairRDD.count());
		actorGreetingsPairRDD.foreach(tuple -> LOGGER.info("({}, {})", tuple._1(), tuple._2()));
		
		/* Sorting by birth year */		
		List<Tuple2<String,List<String>>> sortedByYear = actorGreetingsPairRDD
															.mapToPair(tuple -> {
																Integer key = Integer.parseInt(tuple._2().get(2));
																List<String> value = new ArrayList<>(tuple._2());
																value.add(tuple._1());
																return new Tuple2<>(key, value);
															})
															.sortByKey()
															.collect()
															.stream()
															.map(tuple -> {
																String key = tuple._2().get(3);
																List<String> value = Arrays.asList(tuple._2().get(0), tuple._2().get(1), tuple._2().get(2));
																return new Tuple2<>(key, value);
															})
															.collect(toList());
		
		LOGGER.info("List length: {}", sortedByYear.size());
		sortedByYear.stream().forEach(tuple -> LOGGER.info("({}, {})", tuple._1(), tuple._2()));
	};
	
}
