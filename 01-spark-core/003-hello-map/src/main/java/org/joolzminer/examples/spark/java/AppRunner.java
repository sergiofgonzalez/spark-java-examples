package org.joolzminer.examples.spark.java;

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
								.setAppName("003-hello-map")
								.setMaster("local[*]");
		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {						
			JavaRDD<Integer> numbers = jsc.parallelize(new IntListBuilder()
																.from(10)
																.to(50)
																.by(10)
																.build());
			
			/* iterating over the RDD */
			System.out.println("=== nums");
			numbers.foreach(num -> System.out.println(num));
			
			/* calculating the squares */
			System.out.println("=== nums");
			JavaRDD<Integer> squaredNumbers = numbers.map(number -> number * number);
			squaredNumbers.foreach(num -> System.out.println(num));	
			
			/* reversing (as strings) the squares */
			System.out.println("=== reversed");
			JavaRDD<String> reversed = squaredNumbers
											.map(num -> num.toString())
											.map(numStr -> new StringBuilder(numStr).reverse().toString());
			reversed.foreach(numStr -> System.out.println(numStr));
			
			
			/* materialization in a Java object: getting three largest elements */
			System.out.println("=== materializing top three");
			List<String> topThree = reversed.top(3);
			System.out.println(topThree);
		}
	}	
}
