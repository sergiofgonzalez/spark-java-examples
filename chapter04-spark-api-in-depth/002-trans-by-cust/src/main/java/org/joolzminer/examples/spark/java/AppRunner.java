package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;

import static java.util.Comparator.*;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("002-trans-by-cust")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			/*
			 * Step 1: Create a pair RDD (aka Map) with tuple2 (aka Entry) {CustomerID, trxFields}
			 */
			JavaRDD<String> trxLines = sc.textFile(Paths.get("./src/main/resources", "ch04_data_transactions.txt").toString());
			JavaRDD<String[]> trxFieldsLines = trxLines.map(trxLine -> trxLine.split("#"));
			JavaPairRDD<String, String[]> trxByCustPairRDD = trxFieldsLines.mapToPair(trxFields -> new Tuple2<>(trxFields[2], trxFields));
			
			trxByCustPairRDD.foreach(AppRunner::prettyPrint);
			
			/*
			 * Step 2: Obtain the number of distinct buyers
			 */			
			long numPurchases = trxByCustPairRDD.count();
			
			long numDistinctBuyers = trxByCustPairRDD.keys().distinct().count();
			
			System.out.println(String.format("%d distinct buyers were responsible for yesterdays %d purchases", numDistinctBuyers, numPurchases));
			
			/*
			 * Step 3: Obtain the number of purchases by each customer and verify that the sum of all purchases of each
			 * customer is equal to the number of total purchases.
			 */
			Map<String, Long> custNumPurchasesMap = (Map<String, Long>) (Object) trxByCustPairRDD.countByKey();
			custNumPurchasesMap.entrySet().stream()
				.forEach(entry -> {
					System.out.println(entry.getKey() + ": " + entry.getValue() + " purchase(s)");
				});
			
			long allPurchasesFromMap = custNumPurchasesMap.entrySet().stream()
					.map(Entry::getValue)
					.reduce(0L, Long::sum);
			
			System.out.println("Number of purchases from the map: " + allPurchasesFromMap);
			
			
			/*
			 * Step 4: Find the customer who bought the greatest number of products
			 */
			Stream<Entry<String, Long>> custPurchasesMapSortedByNumPurchases = custNumPurchasesMap.entrySet().stream().sorted(compareByValueDesc);
			Entry<String,Long> custWhoBoughtMore = custPurchasesMapSortedByNumPurchases.findFirst().get();
			System.out.println(String.format("The customer  with id=`%s` bought %d products", custWhoBoughtMore.getKey(), custWhoBoughtMore.getValue()));
		}
	}
	
	private static Comparator<Entry<String, Long>> compareByValue = comparing(Entry::getValue);
	private static Comparator<Entry<String, Long>> compareByValueDesc = compareByValue.reversed();
	
	public static void prettyPrint(Tuple2<String, String[]> trxTuple) {
		System.out.println(trxTuple._1() + "\t:" + Arrays.asList( trxTuple._2()));
	}
}
