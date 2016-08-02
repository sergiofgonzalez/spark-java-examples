package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.util.Comparator.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("e01-trans-by-cust")
								.setMaster("local[*]");


		try (JavaSparkContext jsc = new JavaSparkContext(config)) {
			/*
			 * a) Obtain a JavaPairRDD with elements (customerID, transaction_fields)
			 */
			
			/* a.1) Create an RDD with the lines of the transaction file */
			Path transactionFilePath = Paths.get("./src/main/resources", "ch04_data_transactions.txt");
			JavaRDD<String> transactionFileLinesJavaRDD = jsc.textFile(transactionFilePath.toString());
			LOGGER.debug("transactionFileLines.count={}", transactionFileLinesJavaRDD.count());
			System.out.println("Displaying the first 5 lines of the RDD:");
			transactionFileLinesJavaRDD.take(5).stream().forEach(System.out::println);
			printSeparator();
			
			/* a.2) Split each of the lines into its fields and load in into a JavaPairRDD<String[]> */ 
			JavaRDD<String[]> transactionFieldsJavaRDD = transactionFileLinesJavaRDD.map(line -> line.split("#"));
			transactionFieldsJavaRDD.take(5).stream().forEach(array -> System.out.println(Arrays.asList(array)));
			printSeparator();
			
			/* a.3) Create the JavaPairRDD doing a mapToPair */
			JavaPairRDD<String, String[]> custTransactionsPairRDD = transactionFieldsJavaRDD
																			.mapToPair(fields -> new Tuple2<>(fields[2], fields));
			custTransactionsPairRDD.take(5).stream().forEach(pair -> System.out.println(pair._1() + "=>" + Arrays.asList(pair._2())));
			printSeparator();
			
			/*
			 * b) Find the number of distinct buyers
			 */
			long numPurchases = custTransactionsPairRDD.keys().count();
			long numDistictBuyers = custTransactionsPairRDD.keys().distinct().count();
			LOGGER.debug("Number of distinct buyers: {}; total num of purchases: {}", numDistictBuyers, numPurchases);
			
			
			/*
			 * c) Find the number of purchases for each buyer
			 */
			Map<String, Long> numPurchasesByCustomerMap = (Map<String,Long>) (Object) custTransactionsPairRDD.countByKey();
			numPurchasesByCustomerMap.entrySet().stream()
				.forEach(entry -> System.out.println("[" + entry.getKey() + "] => " + entry.getValue()));
			printSeparator();
			
			/*
			 * d) Find the client who placed the larger amount of purchases (Java)
			 */
			Stream<Entry<String, Long>> sortedNumPurchasesByCustomer = numPurchasesByCustomerMap
																					.entrySet()
																					.stream()
																					.sorted(compareByValueDesc);
			Entry<String, Long> customerWhoPlacedMorePurchases = sortedNumPurchasesByCustomer.findFirst().get();
			LOGGER.debug("Client who placed the larges number of purchases: {} => {}", customerWhoPlacedMorePurchases.getKey(), customerWhoPlacedMorePurchases.getValue()); 
			printSeparator();
			
			
			/*
			 * e) Display the purchased placed by that client
			 */
			List<String[]> purchasesOfCustomerWholePlacedMorePurchases = custTransactionsPairRDD
																			.lookup(customerWhoPlacedMorePurchases.getKey());
			purchasesOfCustomerWholePlacedMorePurchases.stream()
							.forEach(array -> System.out.println(Arrays.asList(array)));
			printSeparator();
			
			/*
			 * f)  Apply 5% discount to the purchases from customers who bought two or more items with ProductID=25
			 */
			JavaPairRDD<String,String[]> custTransactionsWithDiscountAppliedPairRDD =
					custTransactionsPairRDD.mapValues(trxFields -> {
						if ((trxFields[3].equals("25") && (Integer.valueOf(trxFields[4]) >= 2))) {
							Double price = Double.valueOf(trxFields[5]);
							price *= 0.95;
							trxFields[5] = price.toString();
						}
						return trxFields;
					});
			
			/* check that the discount has been applied (only for product==25 and qty >= 2!!! */
			Function<Tuple2<String,String[]>,Boolean> isProduct25 = tuple2 -> tuple2._2()[3].equals("25");
			JavaPairRDD<String,String[]> trxBeforeDiscountJavaPairRDD = custTransactionsPairRDD.filter(isProduct25);
			JavaPairRDD<String,String[]> trxAfterDiscountJavaPairRDD = custTransactionsWithDiscountAppliedPairRDD.filter(isProduct25);
			
			System.out.println("Before ======");
			trxBeforeDiscountJavaPairRDD.collect().forEach(tuple2 -> System.out.println(tuple2._1() + "=>" + Arrays.asList(tuple2._2())));
			System.out.println("After ======");
			trxAfterDiscountJavaPairRDD.collect().forEach(tuple2 -> System.out.println(tuple2._1() + "=>" + Arrays.asList(tuple2._2())));
			printSeparator();
			
			/*
			 * g) Include a complimentary productID = 70 to customers who bought 5 or more productID = 81
			 */
			
			String nowDateStr = LocalDate.now().toString();
			String nowTimeStr = LocalTime.now().format(DateTimeFormatter.ofPattern("h:mm a"));
			JavaPairRDD<String,String[]> custTransactionsWithDiscountAndGiftsPairRDD =
					custTransactionsWithDiscountAppliedPairRDD.flatMapValues(trxFields -> {
						List<String[]> trxFieldsList = new ArrayList<>();
						trxFieldsList.add(trxFields);
						if ((trxFields[3].equals("81")) && Integer.valueOf(trxFields[4]) >= 5) {
							String[] giftTransactionFields = { nowDateStr, nowTimeStr, trxFields[2], "70", "1", "0.0" };
							trxFieldsList.add(giftTransactionFields);
						}
						return trxFieldsList;
					});
			
			LOGGER.debug("Number of final items={}", custTransactionsWithDiscountAndGiftsPairRDD.count());
			
			/*
			 * h) Find the customer who spent the most overall
			 */
			
			/* using reduceByKey */
			JavaPairRDD<String,Double> totalSpentByCustomerPairRDD =
					custTransactionsWithDiscountAndGiftsPairRDD
						.mapValues(trxFields -> Double.valueOf(trxFields[5]))
						.reduceByKey((purchaseTotal1, purchaseTotal2) -> purchaseTotal1 + purchaseTotal2);
			
			Stream<Entry<String,Double>> totalSpentByCustomerMap = totalSpentByCustomerPairRDD
																.collectAsMap()
																.entrySet()
																.stream()
																.sorted(compareByAmountDesc);
			Entry<String,Double> topSpender = totalSpentByCustomerMap.findFirst().get();
			LOGGER.debug("Top spender is {} who spent {}", topSpender.getKey(), topSpender.getValue());
			
			/* using foldByKey */
			JavaPairRDD<String,Double> totalSpentByCustomerAltPairRDD =
					custTransactionsWithDiscountAndGiftsPairRDD
						.mapValues(trxFields -> Double.valueOf(trxFields[5]))
						.foldByKey(0D, (purchaseTotal1, purchaseTotal2) -> purchaseTotal1 + purchaseTotal2);
			
			Stream<Entry<String,Double>> totalSpentByCustomerAltMap = totalSpentByCustomerAltPairRDD
																.collectAsMap()
																.entrySet()
																.stream()
																.sorted(compareByAmountDesc);
			Entry<String,Double> topSpenderAlt = totalSpentByCustomerAltMap.findFirst().get();
			LOGGER.debug("Top spender is {} who spent {}", topSpenderAlt.getKey(), topSpenderAlt.getValue());

			
			/* using aggregateByKey */
			
			
			/* 
			 * i) Include complementary productId 4 to the client who bought the greatest number of products and
			 *    a productId 63 to the client who spent the most
			 */
			List<String[]> complementaryTrx = new ArrayList<String[]>() {{
				add(new String[] { nowDateStr, nowTimeStr, customerWhoPlacedMorePurchases.getKey(), "4", "1", "0.0"});
				add(new String[] { nowDateStr, nowTimeStr, topSpender.getKey(), "63", "1", "0.0"});
			}};
			
			JavaPairRDD<String,String[]> complementaryTrxPairRDD = jsc.parallelize(complementaryTrx)
																		.mapToPair(trxFields -> new Tuple2<>(trxFields[2], trxFields));
			
			JavaPairRDD<String,String[]> finalTransactionsPairRDD = custTransactionsWithDiscountAndGiftsPairRDD
																		.union(complementaryTrxPairRDD);
			
			LOGGER.debug("Final length of transactions file: {}", finalTransactionsPairRDD.count());
			printSeparator();
			
			/*
			 * h) Find the products each customer purchased
			 */
			JavaPairRDD<String,List<String>> productsByCustomerPairRDD = 
					finalTransactionsPairRDD
						.aggregateByKey(new ArrayList<String>(), 										/* zero value fn: empty list  */
						(products, trx) -> { products.add(trx[3]); return products;}, 					/* transform fn : (U, V) => U */ 
						(products1, products2) -> { products1.addAll(products2); return products1;}); 	/* merge fn     : (U, U) => U */
			
			productsByCustomerPairRDD.foreach(tuple2 -> System.out.println(tuple2._1() + "=>" + tuple2._2()));
			printSeparator();
			
			/*
			 * i) Save the final state of the file using the same format
			 */
			LOGGER.debug("Saving results to file system as `final_data_transactions.txt`");
			finalTransactionsPairRDD
				.mapValues(trxFields -> String.join("#", trxFields))
				.saveAsTextFile(Paths.get("./src/main/resources", "final_data_transactions.txt").toString());				
		}		
	}
	
	private static Comparator<Entry<String, Long>> compareByValue = comparing(Entry::getValue);
	private static Comparator<Entry<String, Long>> compareByValueDesc = compareByValue.reversed();
	
	private static Comparator<Entry<String, Double>> compareByAmount = comparing(Entry::getValue);
	private static Comparator<Entry<String, Double>> compareByAmountDesc = compareByAmount.reversed();
	
	private static void printSeparator() {
		System.out.println("=================================================================================");
	}	
}
