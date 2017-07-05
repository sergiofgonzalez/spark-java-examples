package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("004-hello-flatmap")
								.setMaster("local[*]");
		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {						
			/* read the file: each line contains the clients that bought something that day */
			JavaRDD<String> linesRDD = jsc.textFile("./src/main/resources/input-data/client-ids.log");
			System.out.println("=== lines");
			linesRDD.foreach(line -> System.out.println(line));
			
			/* map splitting each line by "," => you get a JavaRDD<String[]> */
			JavaRDD<String[]> clientsPerDayRDD = linesRDD.map(line -> line.split(","));
			System.out.println("=== clientsInDay");
			clientsPerDayRDD.foreach(clientsInDay -> System.out.println(Arrays.asList(clientsInDay)));
			
			/* flatmap splitting each line by "," => you get a JavaRDD<String> */
			JavaRDD<String> clientsInWeekRDD = linesRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator());
			System.out.println("=== clientsInWeek");
			
			/* collect materialized RDD into Java object */
			System.out.println(clientsInWeekRDD.collect());
			
			/* map into JavaRDD<Integer> */
			JavaRDD<Integer> clientIdsInWeekRDD = clientsInWeekRDD.map(clientIdStr -> Integer.parseInt(clientIdStr));			
			
			/* obtain RDD with distinct values */
			JavaRDD<Integer> distinctClientIdsInWeekRDD = clientIdsInWeekRDD.distinct();
			List<Integer> distinctBuyers = distinctClientIdsInWeekRDD.collect();
			System.out.println("=== distinct buyers");
			
			/* count values in RDD */
			System.out.println("distinct buyers count: " + distinctClientIdsInWeekRDD.count());
			System.out.println(distinctBuyers);
		}
	}	
}
