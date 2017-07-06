package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.*;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("006-hello-basic-statistics")
								.setMaster("local[*]");
		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {						

			/* directly creating JavaDoubleRDD */
			JavaDoubleRDD nums = jsc.parallelizeDoubles(
					new IntListBuilder()
							.from(0)
							.to(9)
							.build()
							.stream()
							.map(Integer::doubleValue)
							.collect(toList()));
						
			
			System.out.println("Mean    : " + nums.mean());
			System.out.println("Sum     : " + nums.sum());
			System.out.println("Count   : " + nums.count());
			System.out.println("Variance: " + nums.variance());
			System.out.println("Std Dev : " + nums.stdev());
			System.out.println("Min     : " + nums.min());
			System.out.println("Max     : " + nums.max());
			System.out.println("Stats   : " + nums.stats());
			
			/* Creating a JavaDoubleRDD from JavaRDD */
			JavaRDD<Double> numsRDD = jsc.parallelize(new IntListBuilder().from(0).to(9).build()).map(Integer::doubleValue);
			JavaDoubleRDD doublesRDD = numsRDD.mapToDouble(num -> num);
			System.out.println("Mean    : " + doublesRDD.mean());
			System.out.println("Sum     : " + doublesRDD.sum());
			System.out.println("Count   : " + doublesRDD.count());
			System.out.println("Variance: " + doublesRDD.variance());
			System.out.println("Std Dev : " + doublesRDD.stdev());
			System.out.println("Min     : " + doublesRDD.min());
			System.out.println("Max     : " + doublesRDD.max());
			System.out.println("Stats   : " + doublesRDD.stats());
		}
	}	
}
