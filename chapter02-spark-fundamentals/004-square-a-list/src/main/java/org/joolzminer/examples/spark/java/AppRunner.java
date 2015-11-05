package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

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
								.setAppName("004-square-a-list")
								.setMaster("local[*]");
		
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {		
			JavaRDD<Integer> nums = sparkContext.parallelize(new IntListBuilder()
																	.from(10)
																	.to(50)
																	.by(10)
																	.build());
			
			JavaRDD<Integer> squaredRDD = nums.map(num -> num * num);
			System.out.println("\nSquared List:");
			squaredRDD.foreach(System.out::println);
			
			/* now convert each one to String and reverse */
			JavaRDD<String> reversedSquaredRDD = squaredRDD.map(squared -> new StringBuilder(squared.toString()).reverse().toString());
			reversedSquaredRDD.foreach(System.out::println);
			
			/* displaying info using first and top */
			System.out.println(".first()=" + reversedSquaredRDD.first());
			System.out.println(".top(4)=" + reversedSquaredRDD.top(4));
		}
	}	
	

	
	private static class IntListBuilder {
		private int start;
		private int end;
		private OptionalInt step;
		
		public IntListBuilder() {			
		}
		
		public IntListBuilder from(int start) {
			this.start = start;
			return this;
		}
		
		public IntListBuilder to(int end) {
			this.end = end;
			return this;
		}
		
		public IntListBuilder by(int step) {
			this.step = OptionalInt.of(step);
			return this;
		}
		
		public List<Integer> build() {
			if (!step.isPresent()) {
				step = OptionalInt.of(1);
			}
			
			List<Integer> nums = new ArrayList<>();
			for (int i = start; i <= end; i += step.orElse(1)) {
				nums.add(i);
			}
			return nums;
		}		
	}
}
