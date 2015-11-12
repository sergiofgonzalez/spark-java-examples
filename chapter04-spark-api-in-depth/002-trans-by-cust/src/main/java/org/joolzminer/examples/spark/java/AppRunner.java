package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import static java.util.Comparator.*;
import static java.util.stream.Collectors.*;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings({ "unchecked", "serial" })
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
			String customerIdWhoBoughtMore = custWhoBoughtMore.getKey(); 
			
			/*
			 * Step 5: Find the specific purchases of the customer who bought more products
			 */
			List<String[]> purchasesForCustomerWhoBoughtMore = trxByCustPairRDD.lookup(custWhoBoughtMore.getKey());
			System.out.println("Date, Time, CustomerID, ProductID, Quantity, Price");
			purchasesForCustomerWhoBoughtMore.forEach(purchaseFields -> System.out.println(Arrays.stream(purchaseFields).collect(joining(", "))));
			
			/*
			 * Step 6: Apply a 5% discount to customers who bought two or more items with ProductID=25
			 */
			JavaPairRDD<String, String[]> trxByCustWithDiscountPairRDD = trxByCustPairRDD.mapValues(trxFields -> {
				if (Integer.parseInt(trxFields[3]) == 25 && Integer.parseInt(trxFields[4]) >= 2) {
					trxFields[5] = ((Double)(Double.parseDouble(trxFields[5]) * 0.95)).toString();
				}
				return trxFields;
			});
			trxByCustWithDiscountPairRDD.foreach(AppRunner::prettyPrint);	
			
			// Let's verify the discount has been applied
			JavaPairRDD<String, String[]> trxWithProductBeforeDiscountPairRDD = trxByCustPairRDD.filter(trxTuple2 -> Integer.parseInt(trxTuple2._2()[3]) == 25);
			JavaPairRDD<String, String[]> trxWithProductAfterDiscountPairRDD = trxByCustWithDiscountPairRDD.filter(trxTuple2 -> Integer.parseInt(trxTuple2._2()[3]) == 25);
			
			System.out.println("=================================================================================");
			trxWithProductBeforeDiscountPairRDD.foreach(AppRunner::prettyPrint);
			System.out.println("=================================================================================");
			trxWithProductAfterDiscountPairRDD.foreach(AppRunner::prettyPrint);
			
			/*
			 * Step 7: Add a complimentary productID = 70 for customers who bought more than 5 productIDs = 81
			 */
			
			JavaPairRDD<String, String[]> trxByCustWithGiftsPairRDD = trxByCustPairRDD.flatMapValues(trxFields -> {
				
				List<String[]> customerTrxFieldsList = new ArrayList<>();
				customerTrxFieldsList.add(trxFields);
				
				if (Integer.parseInt(trxFields[3]) == 70 && Integer.parseInt(trxFields[4]) >= 5) {
					String[] giftFields = {trxFields[0], trxFields[1], trxFields[2],
										"81", "1", "0.0"};
					customerTrxFieldsList.add(giftFields);
				}
				
				return customerTrxFieldsList;
			});
			
			// Let's verify the complimentary gifts have been aded
			List<String> dictionaryBuyers = trxByCustPairRDD.filter(trxTuple2 -> Integer.parseInt(trxTuple2._2()[3]) == 70).keys().collect();
			
			JavaPairRDD<String, String[]> trxWithoutGiftsPairRDD = trxByCustPairRDD.filter(trxTuple2 -> dictionaryBuyers.contains(trxTuple2._1()));
			JavaPairRDD<String, String[]> trxWithGiftsPairRDD = trxByCustWithGiftsPairRDD.filter(trxTuple2 -> dictionaryBuyers.contains(trxTuple2._1()));

			System.out.println("=================================================================================");
			trxWithoutGiftsPairRDD.foreach(AppRunner::prettyPrint);
			System.out.println("=================================================================================");
			trxWithGiftsPairRDD.foreach(AppRunner::prettyPrint);

			/*
			 * Step 8: Find the customer who spent the most (using `reduceByKey`)
			 */
			printSeparator();
			JavaPairRDD<String,Double> amountSpentByTrxPairRDD = trxByCustPairRDD.mapValues(trxFields -> Double.parseDouble(trxFields[5]));
			amountSpentByTrxPairRDD.foreach(AppRunner::prettyPrintAmount);
			
			JavaPairRDD<String,Double> amountSpentByCustPairRDD = amountSpentByTrxPairRDD.reduceByKey((trxAmount1, trxAmount2) -> trxAmount1 + trxAmount2);
			printSeparator();
			amountSpentByCustPairRDD.foreach(AppRunner::prettyPrintAmount);
			
			Stream<Entry<String, Double>> amountSpentByCustPairSortByAmountRDD = amountSpentByCustPairRDD
														.collectAsMap()
														.entrySet()
														.stream()
														.sorted(compareByAmountDesc);
			
			printSeparator();
			amountSpentByCustPairSortByAmountRDD.forEach(AppRunner::prettyPrintMapAmountEntry);
			
			String customerIdWhoSpentMore = amountSpentByCustPairRDD
												.collectAsMap()
												.entrySet()
												.stream()
												.sorted(compareByAmountDesc)
												.findFirst()
												.get()
												.getKey();
			
			
			/*
			 * Step 9: Find the customer who spent the most (using `foldByKey`)
			 */
			printSeparator();
			JavaPairRDD<String,Double> amountSpentByCustPairAltRDD = amountSpentByTrxPairRDD.foldByKey(0D, (trxAmount1, trxAmount2) -> trxAmount1 + trxAmount2);
			printSeparator();
			amountSpentByCustPairAltRDD.foreach(AppRunner::prettyPrintAmount);
			
			Stream<Entry<String, Double>> amountSpentByCustPairSortByAmountAltRDD = amountSpentByCustPairAltRDD
														.collectAsMap()
														.entrySet()
														.stream()
														.sorted(compareByAmountDesc);
			
			printSeparator();
			amountSpentByCustPairSortByAmountAltRDD.forEach(AppRunner::prettyPrintMapAmountEntry);		
			
			
			/*
			 * Step 10: Include the complementary gifts (to exemplify how to add to a result):
			 * 	+ productID 4 to the client who bought the greatest number of products
			 *  + productID 63 to the client who spent the most 
			 */
			
			String nowDateStr = LocalDate.now().toString();
			String nowTimeStr = LocalTime.now().format(DateTimeFormatter.ofPattern("h:mm a"));
			
			System.out.println();
			List<String[]> complementaryTrxList = new ArrayList<String[]>() {{ 
				add(new String[] {nowDateStr, nowTimeStr, customerIdWhoBoughtMore, "4", "1", "0.00"});
				add(new String[] {nowDateStr, nowTimeStr, customerIdWhoSpentMore, "63", "1", "0.00"});
			}};
			
			printSeparator();
			System.out.println("Complementary Transactions:");
			complementaryTrxList.forEach(trx -> System.out.println(Arrays.asList(trx)));
			
			JavaRDD<String[]> complementaryRDD = sc.parallelize(complementaryTrxList);
			JavaPairRDD<String, String[]> complementaryByCustPairRDD = complementaryRDD.mapToPair(trx -> new Tuple2<>(trx[2], trx));
			
			
			JavaPairRDD<String, String[]> postProcessedTrxPairRDD = trxByCustPairRDD
					.union(complementaryByCustPairRDD);
			
			System.out.println("Total Transactions: " + postProcessedTrxPairRDD.count());
			
			// Dumping to output file
			JavaRDD<String[]> postProcessedTrxRDD = postProcessedTrxPairRDD.map(entry -> (entry._2()));
			JavaRDD<String> postProcessedTrxLineRDD = postProcessedTrxRDD.map(trx -> Arrays.stream(trx).collect(joining("#")));
			
			postProcessedTrxLineRDD.saveAsTextFile(Paths.get("./src/main/resources", "ch04_data_transactions.postprocessed.txt").toString());
		
		
			/*
			 * Step 11: Find the products each customer purchased
			 */
			
			printSeparator();
			JavaPairRDD<String, List<String>> productsByCustPairRDD = trxByCustPairRDD
					.aggregateByKey(new ArrayList<String>(), 
							(products, trx) ->  { products.add(trx[3]); return products; }, 
							(products1, products2) -> { products1.addAll(products2); return products1; });
			
			productsByCustPairRDD.foreach(entry -> {
				System.out.println(entry._1() + ":" + entry._2());
			});
		}
	}


	private static Comparator<Entry<String, Long>> compareByValue = comparing(Entry::getValue);
	private static Comparator<Entry<String, Long>> compareByValueDesc = compareByValue.reversed();
	
	private static Comparator<Entry<String, Double>> compareByAmount = comparing(Entry::getValue);
	private static Comparator<Entry<String, Double>> compareByAmountDesc = compareByAmount.reversed();
	
	public static void prettyPrint(Tuple2<String, String[]> trxTuple) {
		System.out.println(trxTuple._1() + "\t:" + Arrays.asList( trxTuple._2()));
	}
	
	public static void prettyPrintAmount(Tuple2<String, Double> trxTuple) {
		System.out.println(trxTuple._1() + "\t: $" + trxTuple._2());
	}	
	
	
	public static void prettyPrintMapAmountEntry(Entry<String, Double> entry) {
		System.out.println(entry.getKey() + ", " + entry.getValue());
	}
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
