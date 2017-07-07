package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("008-hello-approx-sum-and-mean")
								.setMaster("local[*]");
		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {		
			JavaDoubleRDD valuesRDD = jsc.parallelizeDoubles(new ArrayList<Double>(Integer.MAX_VALUE / 100) {{ 
				for(Integer i = 0; i < Integer.MAX_VALUE / 100; i++) {
					add(i.doubleValue());
				}
			}});
		
			PartialResult<BoundedDouble> approxSum = valuesRDD.sumApprox(1000L, 0.75);
			PartialResult<BoundedDouble> approxMean = valuesRDD.meanApprox(1000L, 0.75);
			System.out.println("approxSum=" + approxSum.getFinalValue());
			System.out.println("approxMean=" + approxMean.getFinalValue());
		}
	}
}
