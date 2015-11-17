package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("004-trans-products-join")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			/*
			 * Step 1: Create a pair RDD (aka Map) with tuple2 (aka Entry) {CustomerID, trxFields}
			 */
			JavaRDD<String> trxLines = sc.textFile(Paths.get("./src/main/resources", "ch04_data_transactions.txt").toString());
			JavaRDD<String[]> trxFieldsLines = trxLines.map(trxLine -> trxLine.split("#"));
			JavaPairRDD<String, String[]> trxByCustPairRDD = trxFieldsLines.mapToPair(trxFields -> new Tuple2<>(trxFields[2], trxFields));
			
			trxByCustPairRDD.foreach(AppRunner::prettyPrint);
			
			printSeparator();
			
			/*
			 * Step 2: Create a pair RDD with tuple2 {ProductID, productFields}
			 */
			JavaRDD<String> productLines = sc.textFile(Paths.get("./src/main/resources", "ch04_data_products.txt").toString());
			JavaRDD<String[]> productFieldsLines = productLines.map(trxLine -> trxLine.split("#"));
			JavaPairRDD<String, String[]> productsPairRDD = productFieldsLines.mapToPair(productFields -> new Tuple2<>(productFields[0], productFields));
			
			productsPairRDD.foreach(AppRunner::prettyPrint);
			
			printSeparator();
			
			/*
			 * Step 3: Create a pair RDD with tuple2 {ProductID, transaction fields}
			 */
			JavaPairRDD<String, String[]> transByProductPairRDD = trxByCustPairRDD.mapToPair(trxTuple -> new Tuple2<>(trxTuple._2()[3], trxTuple._2()));
			transByProductPairRDD.foreach(AppRunner::prettyPrint);
			printSeparator();
			
			/*
			 * Step 4: Create a pair RDD with tuple2 : {ProductID, amount}
			 */
			JavaPairRDD<String, Double> amountByProductPairRDD = transByProductPairRDD.mapValues(trxFields -> Double.parseDouble(trxFields[5]));
			amountByProductPairRDD.foreach(AppRunner::prettyPrintAmount);
			printSeparator();
			
			/*
			 * Step 5: Create a pair RDD with the totals by product sorted by amount
			 */
			JavaPairRDD<String, Double> totalsByProductPairRDD = amountByProductPairRDD.reduceByKey((amt1, amt2) -> amt1 + amt2);
			
			/* sortBy is unavailable on JavaPairRDD */
			JavaPairRDD<Double, String> totalsByProductSortedByAmountPairRDD = 
					totalsByProductPairRDD
						.mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
						.sortByKey();
			
			/* Warning: foreach on RDD may break the ordering while printing the results, that's why we collect the results */
			List<Tuple2<Double, String>> totalsByProductInverseList = totalsByProductSortedByAmountPairRDD.collect();
			totalsByProductInverseList.stream()
				.forEach(AppRunner::prettyPrintAmountInverseMap);			
			printSeparator();
			
			/* alternatively, you can use JavaRDD<Tuple2<>> */
			JavaRDD<Tuple2<String, Double>> totalsByProductRDD = totalsByProductPairRDD.map(tuple2 -> new Tuple2<>(tuple2._1(), tuple2._2()));
			JavaRDD<Tuple2<String, Double>> totalsByProductSortedByAmountRDD =
					totalsByProductRDD.sortBy(compareByAmount, true, 2);
			
			List<Tuple2<String, Double>> totalsByProductList = totalsByProductSortedByAmountRDD.collect();
			totalsByProductList.forEach(AppRunner::prettyPrintAmount);
			printSeparator();
			
			/*
			 * Step 6: Obtain the pair RDD resulting of joining totals by product and products.
			 */			
			JavaPairRDD<String, Tuple2<Double, String[]>> productTotalsAndAttributesPairRDD =
					totalsByProductPairRDD.join(productsPairRDD);
			
			JavaRDD<Tuple2<String, Tuple2<Double, String[]>>> productTotalsAndAttributesRDD =
					productTotalsAndAttributesPairRDD.map(item -> new Tuple2<>(item._1(), new Tuple2<>(item._2()._1(), item._2()._2())));
			JavaRDD<Tuple2<String, Tuple2<Double, String[]>>> productTotalsAndAttributesSortedByAmountRDD =
					productTotalsAndAttributesRDD.sortBy(tuple2 -> tuple2._2()._1(), 
																true, 10);
			
			List<Tuple2<String, Tuple2<Double, String[]>>> productTotalsAndAttributesList = 
					productTotalsAndAttributesSortedByAmountRDD.collect();
			
			productTotalsAndAttributesList.stream()
				.forEach(AppRunner::prettyPrintTotalsAndAttributes);
			printSeparator();						
		}
	}

	private static Function<Tuple2<String, Double>, Double> compareByAmount = tuple2 -> tuple2._2();
	
	
	private static void prettyPrint(Tuple2<String, String[]> trxTuple) {
		System.out.println(trxTuple._1() + "\t:" + Arrays.asList( trxTuple._2()));
	}
	
	private static void prettyPrintAmount(Tuple2<String, Double> trxTuple) {
		System.out.println(trxTuple._1() + "\t: $" + new DecimalFormat("###.00").format(trxTuple._2()));
	}

	private static void prettyPrintAmountInverseMap(Tuple2<Double, String> trxTuple) {
		System.out.println(trxTuple._2() + "\t: $" + new DecimalFormat("###.00").format(trxTuple._1()));
	}
	
	private  static void prettyPrintTotalsAndAttributes(Tuple2<String, Tuple2<Double, String[]>> item) {
		System.out.println(item._1() + "\t: $" + new DecimalFormat("###.00").format(item._2()._1()) + "\t" + Arrays.asList(item._2()._2()));
	}
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
