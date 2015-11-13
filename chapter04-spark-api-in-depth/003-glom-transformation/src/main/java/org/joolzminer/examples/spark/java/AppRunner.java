package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	

	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("003-glom-transformation")
								.setMaster("local[*]");

		/* fill a list with 500 random ints */
		Random random = new Random();
		
		List<Integer> nums = new ArrayList<>(500);
		for (int i = 0; i < 500; i++) {
			nums.add(random.nextInt(100));
		}
		
		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			JavaRDD<Integer> numsRDD = sc.parallelize(nums, 30);
			System.out.println("numsRdd.count: " + numsRDD.count());
			
			JavaRDD<List<Integer>> glommedNumsRDD = numsRDD.glom();
			System.out.println("glommedNumsRdd.count: " + glommedNumsRDD.count());
			
			glommedNumsRDD.foreach(System.out::println);
		}
	}



	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
