package com.github.sergiofgonzalez.examples.spark.java;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sergiofgonzalez.examples.spark.java.utils.IntListBuilder;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("005-hello-sampling")
								.setMaster("local[*]");
		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {						
			JavaRDD<Integer> nums = jsc.parallelize(new IntListBuilder().from(0).to(9).build());

			
			/* sampling with replacement, fraction = 1 means each element is expected to be sampled once */
			for (int i = 0; i < 5; i++) {
				JavaRDD<Integer> sample1 = nums.sample(true, 1);
				System.out.println("sample1: " + sample1.collect());
			}
			
			for (int i = 0; i < 5; i++) {
				JavaRDD<Integer> sample2 = nums.sample(true, 2);
				System.out.println("sample2: " + sample2.collect());
			}			
			
			for (int i = 0; i < 5; i++) {
				JavaRDD<Integer> sample3 = nums.sample(true, .5);
				System.out.println("sample3: " + sample3.collect());
			}			
			
			/* sampling without replacement, fraction is the sampling probability for any element */
			for (int i = 0; i < 5; i++) {
				JavaRDD<Integer> sample4 = nums.sample(false, 1);
				System.out.println("sample4: " + sample4.collect());
			}			
			
			for (int i = 0; i < 5; i++) {
				JavaRDD<Integer> sample5 = nums.sample(false, 0.33);
				System.out.println("sample5: " + sample5.collect());
			}	
			
			
			/* takeSample */
			for (int i = 0; i < 5; i++) {
				List<Integer> sample6 = nums.takeSample(false, 3);
				System.out.println("sample6: " + sample6);
			}				
			
			for (int i = 0; i < 5; i++) {
				List<Integer> sample7 = nums.takeSample(true, 10);
				System.out.println("sample7: " + sample7);
			}							
		}
	}	
}
