package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import static java.lang.Math.*;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("008-customer-statistics")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			/*
			 * Create a pair RDD (aka Map) with tuple2 (aka Entry) {CustomerID, trxFields}
			 */
			JavaRDD<String> trxLines = sc.textFile(Paths.get("./src/main/resources", "ch04_data_transactions.txt").toString());
			JavaRDD<String[]> trxFieldsLines = trxLines.map(trxLine -> trxLine.split("#"));
			JavaPairRDD<String, String[]> trxByCustPairRDD = trxFieldsLines.mapToPair(trxFields -> new Tuple2<>(trxFields[2], trxFields));
			
			trxByCustPairRDD.foreach(AppRunner::prettyPrint);
			
			/*
			 * Find the statistics min, max, qty, total amount, average per trans 
			 */
			
			/* create the first combined value from the first key's value in each partition */
			Function<String[], Tuple4<Double, Double, Integer, Double>> createCombiner = 
					trxFields -> {
						Double total = Double.parseDouble(trxFields[5]);
						Integer qty = Integer.parseInt(trxFields[4]);
						return new Tuple4<>(total / qty, total / qty, qty, total);
					};
			
			/* merge additional key values with the combined value, inside a single partition */
			Function2<Tuple4<Double, Double, Integer, Double>, String[], Tuple4<Double, Double, Integer, Double>> mergeValue =
					(tuple4, trxFields) -> {
						Double total = Double.parseDouble(trxFields[5]);
						Integer qty = Integer.parseInt(trxFields[4]);
						return new Tuple4<>(min(tuple4._1(), total / qty), max(tuple4._2(), total / qty), tuple4._3() + qty, tuple4._4() + total);
					};
					
			/* merge combined values themselves across partitions */
			Function2<Tuple4<Double, Double, Integer, Double>, Tuple4<Double, Double, Integer, Double>, Tuple4<Double, Double, Integer, Double>> mergeCombiners =
					(tuple4a, tuple4b) -> new Tuple4<>(min(tuple4a._1(), tuple4b._1()), 
														max(tuple4a._2(), tuple4b._2()),
														tuple4a._3() + tuple4b._3(),
														tuple4a._4() + tuple4b._4());
			
			/* If the partitioner is the same as the existing partitioner, there will be no need for shuffle */
			Partitioner partitioner = new HashPartitioner(trxByCustPairRDD.partitions().size());
					
			JavaPairRDD<String, Tuple4<Double, Double, Integer, Double>> combinedPairRDD = trxByCustPairRDD.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner);
			
			JavaPairRDD<String, Tuple5<Double, Double, Integer, Double, Double>> statisticsByCust =
					combinedPairRDD.mapValues(tuple4 -> new Tuple5<>(tuple4._1(), tuple4._2(), tuple4._3(), tuple4._4(), tuple4._4() / tuple4._3()));
			
			System.out.println();
			statisticsByCust.collect().forEach(AppRunner::prettyPrintStatistics);
			printSeparator();
		}
			
	}

	
	private static void prettyPrintStatistics(Tuple2<String, Tuple5<Double, Double, Integer, Double, Double>> item) {
		System.out.println("CustomerID: " + item._1() + ":");
		System.out.println("\tmin=" + new DecimalFormat("###.00").format(item._2()._1()));
		System.out.println("\tmax=" + new DecimalFormat("###.00").format(item._2()._2()));
		System.out.println("\tqty=" + item._2()._3());
		System.out.println("\ttot=" + new DecimalFormat("###.00").format(item._2()._4()));
		System.out.println("\tavg=" + new DecimalFormat("###.00").format(item._2()._5()));
	}
	
	private static void prettyPrint(Tuple2<String, String[]> trxTuple) {
		System.out.println(trxTuple._1() + "\t:" + Arrays.asList( trxTuple._2()));
	}
	
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
