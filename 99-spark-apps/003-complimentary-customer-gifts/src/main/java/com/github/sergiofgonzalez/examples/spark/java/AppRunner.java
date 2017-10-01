package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;
import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;
import static java.util.stream.Collectors.*;

import java.io.File;
import java.util.ArrayList;

import static java.util.Comparator.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("PurchaseLogProcessingApp")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(purchaseLogProcessing)
						.submit());		
	}
	
	private static final Comparator<Entry<Integer, Long>> compareByValue = comparing(Entry::getValue);
	private static final Comparator<Entry<Integer, Long>> compareByValueReversed = compareByValue.reversed();	
		
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> purchaseLogProcessing = (spark, sparkContext) -> {
		
		/* Load the file */
		JavaRDD<String> linesRDD = sparkContext.textFile(wconf().get("files.input.purchases_data"));
		
		/* Extract the individual fields */
		JavaRDD<List<String>> purchaseTransactionsRDD = linesRDD.map(line -> Arrays.asList(line.split("#")));
		
		/* Create a pairRDD with (custID, purchase) */		
		JavaPairRDD<Integer,List<String>> purchaseTransactionsByCustomerRDD = purchaseTransactionsRDD.mapToPair(purchaseTransactionFields -> new Tuple2<Integer,List<String>>(Integer.parseInt(purchaseTransactionFields.get(2)), purchaseTransactionFields));
		
		/* Obtain how many different customers bought products */
		Long differentBuyersCount = purchaseTransactionsByCustomerRDD.keys().distinct().count();
		LOGGER.info("Number of different buyers in Purchase Log: {}", differentBuyersCount);
		
	
		/* Obtain number of purchases by customer (materialized) */
		Map<Integer, Long> numPurchasesByCustomerMap = purchaseTransactionsByCustomerRDD.countByKey();
		LOGGER.info("Number of purchases by Customer in Purchase Log: {}", numPurchasesByCustomerMap);
		
		/* Verify that the sum of all purchases is equal to the number of records in the log */
		Long numPurchases = numPurchasesByCustomerMap.entrySet().stream()
								.map(entry -> entry.getValue())
								.reduce(0L, (acc, elem) -> acc + elem);
		LOGGER.info("Total number of purchases (should be 1000): {}", numPurchases);
								
		/* Obtain the customer who placed the most purchases */
		Entry<Integer, Long> maxPurchasesCustId = numPurchasesByCustomerMap.entrySet().stream()
								.sorted(compareByValueReversed)
								.collect(toList())
								.get(0);
		LOGGER.info("ClientID who placed the most purchases: ({}, {})", maxPurchasesCustId.getKey(), maxPurchasesCustId.getValue());
		
		/* Specific transactions for the customer who placed the most purchases */
		List<List<String>> purchasesForClient = purchaseTransactionsByCustomerRDD.lookup(maxPurchasesCustId.getKey());		
		LOGGER.info("Purchases for client who placed the most purchases ({} purchases): {}", purchasesForClient.size(), purchasesForClient);
		
		/* Apply 5% discount to those who bought 2 or more "Barbie Shopping Mall Playset" products */
		JavaRDD<String> productsRDD = sparkContext.textFile(wconf().get("files.input.products_data"));
		JavaPairRDD<String,List<String>> productsByNameRDD = productsRDD
																.map(line -> line.split("#"))
																.map(fieldsArray -> Arrays.asList(fieldsArray))
																.mapToPair(fields -> new Tuple2<String,List<String>>(fields.get(1), fields));
		
		Integer barbieProductId = findProductIdByName(productsByNameRDD, "Barbie Shopping Mall Playset");
		LOGGER.info("Barbie Shopping Mall Playset Product ID: {}", barbieProductId);
		
		purchaseTransactionsByCustomerRDD = purchaseTransactionsByCustomerRDD.mapValues(purchase -> {
			if (Integer.parseInt(purchase.get(3)) == barbieProductId && Integer.parseInt(purchase.get(4)) > 1) {
				Double prevAmount = Double.parseDouble(purchase.get(5));
				Double currAmount = prevAmount * 0.95;
				LOGGER.debug("Applying discount for customer ID {} : {} to {}", purchase.get(2), prevAmount, currAmount);
				purchase.set(5, currAmount.toString());
			}
			return purchase;
		});
		
		/* Add a complimentary toothbrush to customers who bought 5 or more dictionaries */
		Integer toothbrushProductId = findProductIdByName(productsByNameRDD, "Toothbrush");
		LOGGER.info("Toothbrush Product ID: {}", toothbrushProductId);
		
		Integer dictionaryProductId = findProductIdByName(productsByNameRDD, "Dictionary");
		LOGGER.info("Dictionary Product ID: {}", dictionaryProductId);

		/* flatMapValues allows to to change the number of elements associated to a key: explore in the core examples */
		purchaseTransactionsByCustomerRDD = purchaseTransactionsByCustomerRDD
												.flatMapValues(purchase -> {
													List<List<String>> resultPurchases = new ArrayList<>();
													resultPurchases.add(purchase);
													if (Integer.parseInt(purchase.get(3)) == dictionaryProductId && Integer.parseInt(purchase.get(4)) >= 5) {
														List<String> complimentaryToothbrushTransaction = new ArrayList<>();
														complimentaryToothbrushTransaction.add(purchase.get(0));
														complimentaryToothbrushTransaction.add(purchase.get(1));
														complimentaryToothbrushTransaction.add(purchase.get(2));
														complimentaryToothbrushTransaction.add("70");
														complimentaryToothbrushTransaction.add("1");
														complimentaryToothbrushTransaction.add("0.00");
														resultPurchases.add(complimentaryToothbrushTransaction);
														LOGGER.debug("A complimentary Toothbrush added for customer ID {} who bought {} products", purchase.get(2), purchase.get(4));
													}
													return resultPurchases;
												});
		LOGGER.info("Purchase Transactions after adding complimentary Toothbrush: {} ", purchaseTransactionsByCustomerRDD.count());
		
		/* finding the customer who spent the most */
		JavaPairRDD<Integer, Double> purchaseAmountByCustomerRDD = purchaseTransactionsByCustomerRDD.mapValues(purchaseTransaction -> Double.parseDouble(purchaseTransaction.get(5)));
		JavaPairRDD<Integer, Double> totalAmountByCustomerRDD = purchaseAmountByCustomerRDD
																	.foldByKey(0.0, (acc, purchaseAmount) -> acc + purchaseAmount);
		
		/* note that to sort a JavaPairRDD by value you need to swap the tuple2 elements and then sortByKey */
		JavaPairRDD<Double, Integer> customerByTotalAmountRDD = totalAmountByCustomerRDD
																	.mapToPair(tuple2 -> new Tuple2<Double,Integer>(tuple2._2(), tuple2._1()))
																	.sortByKey(false);

		Tuple2<Double,Integer> amountCustomerIdTuple2WhoSpentTheMost = customerByTotalAmountRDD.collect().get(0);
		LOGGER.info("The customer {} spent ${}", amountCustomerIdTuple2WhoSpentTheMost._2(), amountCustomerIdTuple2WhoSpentTheMost._1());
		
		/* Add Pajamas to the customer who spent the most */
		Integer pajamasProductId = findProductIdByName(productsByNameRDD, "Pajamas");
		LOGGER.info("Pajamas Product ID: {}", pajamasProductId);
		
		List<String> complimentaryTransactionFields = new ArrayList<>();
		complimentaryTransactionFields.add("2015-03-30");
		complimentaryTransactionFields.add("11:59 PM");
		complimentaryTransactionFields.add(amountCustomerIdTuple2WhoSpentTheMost._2().toString());
		complimentaryTransactionFields.add(pajamasProductId.toString());
		complimentaryTransactionFields.add("1");
		complimentaryTransactionFields.add("0.00");
		
		/* prepare the element to wrap it in an RDD using parallelize */
		List<List<String>> complimentaryTransactions = new ArrayList<>();
		complimentaryTransactions.add(complimentaryTransactionFields);
		
		/* Add them to the purchase log */					
		purchaseTransactionsByCustomerRDD = purchaseTransactionsByCustomerRDD
												.union(sparkContext.parallelize(complimentaryTransactions)
																	.mapToPair(purchaseTransaction -> {
																		LOGGER.debug("PurchaseTransaction={}", purchaseTransaction);
																		return new Tuple2<Integer,List<String>>(Integer.parseInt(purchaseTransaction.get(2)), purchaseTransaction);
																	}));
	
		/* Save the result file */
		
		FileUtils.deleteQuietly(new File(wconf().get("files.output.purchases_data"))); // obviously, this only works if local file system is used!!
		purchaseTransactionsByCustomerRDD
				.map(tuple2 -> tuple2._2())
				.map(purchase -> String.join("#", purchase))	
				.coalesce(1)
				.saveAsTextFile(wconf().get("files.output.purchases_data"));
	};

	
	private static Integer findProductIdByName(JavaPairRDD<String,List<String>> productsByNameRDD, String name) {
		return Integer.parseInt(productsByNameRDD.lookup(name)
				.get(0)
				.get(0));
	}

	
}
