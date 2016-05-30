package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joolzminer.examples.spark.java.utils.IntListBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import static java.util.stream.Collectors.*;


public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("008-basic-statistics")
								.setMaster("local[*]");

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {		
			JavaDoubleRDD numsRDD = sparkContext.parallelizeDoubles(IntListBuilder
																			.getInts()
																			.from(1).to(10)
																			.build()
																			.stream()
																			.map(Integer::doubleValue)
																			.collect(toList()));

			printSeparator();
			System.out.println("Mean    : " + numsRDD.mean());
			System.out.println("Sum     : " + numsRDD.sum());
			System.out.println("Count   : " + numsRDD.count());
			System.out.println("Variance: " + numsRDD.variance());
			System.out.println("Std Dev : " + numsRDD.stdev());
			System.out.println("Min     : " + numsRDD.min());
			System.out.println("Max     : " + numsRDD.max());
			System.out.println("Stats   : " + numsRDD.stats());			
			printSeparator();
			
			
			/* Get histogram with fixed intervals */
			System.out.println("nums: " + numsRDD.collect());
			double[] intervals = { 1.0, 5.0, 10.0 };
			long[] buckets = numsRDD.histogram(intervals);
			prettyPrintHistogramData(intervals, buckets);
			printSeparator();
			
			/* Get histogram with fixed number of intervals */
			System.out.println("nums: " + numsRDD.collect());

			
			Tuple2<double[], long[]> bucketTuples = numsRDD.histogram(2);
			prettyPrintHistogramData(bucketTuples);
			printSeparator();
			
		}			
	}	
	

	
	private static final void prettyPrintHistogramData(double[] intervals, long[] buckets) {
		if (intervals.length != buckets.length + 1) {
			LOGGER.error("Incorrect size: intervals.length={}, buckets.length={}; buckets.length should be intervals.length - 1"); 
			throw new IllegalArgumentException("Incorrect size: intervals and buckets size do not match expectations");
		}
		for (int i = 0; i < intervals.length - 2; i++) {
			System.out.println("[" + intervals[i] + ", " + intervals[i+1] + ") : " + buckets[i]);
		}
		System.out.println("[" + intervals[intervals.length - 2] + ", " + intervals[ intervals.length - 1] + "] : " + buckets[intervals.length - 2]);		
	}
	
	private static final void prettyPrintHistogramData(Tuple2<double[], long[]> bucketTuples) {
		prettyPrintHistogramData(bucketTuples._1(), bucketTuples._2());
	}
	
	private static final void printSeparator() {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	}
}
