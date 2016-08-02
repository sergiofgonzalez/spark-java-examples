package org.joolzminer.examples.spark.java;

import org.apache.spark.Accumulable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joolzminer.examples.spark.java.accumulables.IntegerAverageAccumulableParam;
import org.joolzminer.examples.spark.java.accumulables.IntegerSumAccumulableParam;
import org.joolzminer.examples.spark.java.accumulables.MetricsAvgAccumulableParam;
import org.joolzminer.examples.spark.java.accumulables.MetricsSumAccumulableParam;
import org.joolzminer.examples.spark.java.accumulables.MetricsSumBySiteAccumulableParam;
import org.joolzminer.examples.spark.java.utils.IntListBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("e05 moving24HourAvg using RDDs")
								.setMaster("local[*]");

		
		try (JavaSparkContext jsc = new JavaSparkContext(config)) {
 
			/* 1.1: Calculate the sum of the records of an RDD consisting in the sequence from 1 to 1_000_000 */
			
			// Step 0: create the RDD with the input data
			JavaRDD<Integer> numsRDD = jsc.parallelize(IntListBuilder.getInts().from(1).to(1_000_000).build());

			// Step 1: create the Accumulable<R, T> where T is the element being added and R is the result type 
			Accumulable<Long, Integer> accumulableForSum = jsc.accumulable(new Long(0L), new IntegerSumAccumulableParam());
			
			// Step 2: perform the traversing of the RDD accumulating the values
			numsRDD.foreach(accumulableForSum::add);
			
			// Step 3: Display the results
			LOGGER.debug("Sum of the numbers in the RDD={}", accumulableForSum.value());
			
	
			/* 1.2: Calculate the average of the records of an RDD consisting in the sequence from 1 to 1_000_000 */
			
			// Step 1: create the Accumulable<R, T> where T is the element being added and R is the result type 
			Accumulable<Tuple2<Long,Long>, Integer> accumulableForAverage = jsc.accumulable(new Tuple2<>(0L,0L), new IntegerAverageAccumulableParam());
			
			// Step 2: perform the traversing of the RDD accumulating the values
			numsRDD.foreach(accumulableForAverage::add);
			
			// Step 3: Display the results
			LOGGER.debug("Average of the numbers in the RDD={}/{}={}", accumulableForAverage.value()._2(), accumulableForAverage.value()._1(), (double)accumulableForAverage.value()._2() / (double) accumulableForAverage.value()._1());			

			/* 2.1: Calculate the sum of the records of an RDD consisting in the sequence from 1 to 1_000_000 */
			
			// Step 0: create the RDD with the input data
			JavaRDD<String> dataLines = jsc.textFile(Paths.get("./src/main/resources", "sample-data.csv").toString());
			JavaRDD<String[]> dataLineFieldsRDD = dataLines.map(line -> line.split(","));
			
			// Step 1: create the Accumulable<R, T> where T is the element being added and R is the result type 
			Accumulable<List<Double>, String[]> accumulableForMetricSum = jsc.accumulable(new ArrayList<Double>(), new MetricsSumAccumulableParam());
			
			// Step 2: perform the traversing of the RDD accumulating the values
			dataLineFieldsRDD.foreach(accumulableForMetricSum::add);
			
			// Step 3: Display the results
			LOGGER.debug("Sum of the metrics in the RDD per column={}", accumulableForMetricSum.value());
			
			/* 2.2: Calculate average of all parameters (columns 2 thru 7) */
			
			// Step 1: create the Accumulable<R, T> where T is the element being added and R is the result type 
			Accumulable<Tuple2<Long,List<Double>>, String[]> accumulableForMetricAvg = jsc.accumulable(new Tuple2<>(0L, new ArrayList<Double>()), new MetricsAvgAccumulableParam());
			
			// Step 2: perform the traversing of the RDD accumulating the values
			dataLineFieldsRDD.foreach(accumulableForMetricAvg::add);
			
			// Step 3: Display the results
			LOGGER.debug("Avg of the metrics in the RDD per column: num Values={}, sum per column={}", accumulableForMetricAvg.value()._1(), accumulableForMetricAvg.value()._2());
			accumulableForMetricAvg.value()._2().stream()
				.forEach(calculatedSum -> LOGGER.debug("AVG={}", (double) calculatedSum / (double) accumulableForMetricAvg.value()._1()));

			/* 2.3: Calculate the sum of all parameters (columns 2 thru 7) grouped by the first column */
			
			// Step 1: create the Accumulable<R, T> where T is the element being added and R is the result type 
			Accumulable<Map<String,List<Double>>, String[]> accumulableForMetricSumBySite = jsc.accumulable(new HashMap<>(), new MetricsSumBySiteAccumulableParam());
			
			// Step 2: perform the traversing of the RDD accumulating the values
			dataLineFieldsRDD.foreach(accumulableForMetricSumBySite::add);
			
			// Step 3: Display the results
			LOGGER.debug("Sum of the metrics in the RDD per column grouped by site: {}", accumulableForMetricSumBySite.value());
		}
			
	}
	
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
