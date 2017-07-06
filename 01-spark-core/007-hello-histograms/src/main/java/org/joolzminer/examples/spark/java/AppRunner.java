package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.util.Arrays;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("007-hello-histograms")
								.setMaster("local[*]");
		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {						
			JavaDoubleRDD valuesRDD = jsc.parallelizeDoubles(Arrays.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0));
						
			/* single interval [0.0, 10.0] */
			long[] histogramData = valuesRDD.histogram(new double[]{0.0, 10.0});
						
			System.out.println("=== interval [0.0, 10.0)");
			Arrays.stream(histogramData).forEach(bucket -> System.out.println(bucket));
			
			/* two equal intervals [0.0, 5.0) and [5.0, 10.0) */
			histogramData = valuesRDD.histogram(new double[]{0.0, 5.0, 10.0});
						
			System.out.println("=== interval [0.0, 5.0) and [5.0, 10.0)");
			Arrays.stream(histogramData).forEach(bucket -> System.out.println(bucket));
			
			/* three equal intervals [0.0, 3.33), [3.33, 6.66), [6.66, 10.0) */
			histogramData = valuesRDD.histogram(new double[]{0.0, 3.33, 6.66, 10.0});
						
			System.out.println("=== interval [0.0, 3.33), [3.33, 6.66), [6.66, 10.0)");
			Arrays.stream(histogramData).forEach(bucket -> System.out.println(bucket));
			
			
			/* You can also specify a number of intervals in which the data must be splitted */
			Tuple2<double[], long[]> buckets = valuesRDD.histogram(5);
			System.out.println("=== 5 intervals");
			prettyPrintTuple2(buckets);
			
		}
	}
	
	private static final void prettyPrintTuple2(Tuple2<double[], long[]> histogramData) {
		for (int i = 0; i < histogramData._2().length; i++) {
			System.out.println("[" + histogramData._1()[i] + ", " + histogramData._1()[i + 1] + "): " + histogramData._2()[i]);
		}
	}
}
