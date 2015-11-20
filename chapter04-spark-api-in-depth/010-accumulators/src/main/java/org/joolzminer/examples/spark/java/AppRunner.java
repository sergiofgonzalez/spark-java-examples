package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("010-accumulators")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			/* Accumulator */
			Accumulator<Integer> accumulator = sc.accumulator(0, "my-accumulator");
			JavaRDD<Integer> list = sc.parallelize(new ArrayList<Integer>() {{
				for (int i = 0; i < 1_000_000; i++) {
					add(i);
				}
			}}); 
			
			list.foreach(i -> accumulator.add(1));
			
			System.out.println("Querying value from the driver: " + accumulator.value());
			
			try {
				list.foreach(i -> System.out.println("From the worker: " + accumulator.value()));
			} catch (Throwable t) {
				System.out.println("Exception caught: " + t.getMessage());
			}
			printSeparator();
		}
			
	}
	
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
