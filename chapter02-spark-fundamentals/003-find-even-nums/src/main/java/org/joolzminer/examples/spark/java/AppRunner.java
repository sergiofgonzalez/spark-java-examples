package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
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
								.setAppName("003-find-even-nums")
								.setMaster("local[*]");
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {		
			JavaRDD<Integer> nums = sparkContext.parallelize(fromTo(1, 10));
			
			JavaRDD<Integer> evenNums = nums.filter(num -> num % 2 == 0);
			System.out.println("\nEven numbers:");
			evenNums.foreach(System.out::println);
		}
	}	
	
	private static List<Integer> fromTo(int init, int end) {
		List<Integer> result = new ArrayList<>(end - init + 1);
		for (int i = init; i <= end; i++) {
			result.add(i);
		}
		return result;
	}
}
