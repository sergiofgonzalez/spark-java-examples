package org.joolzminer.examples.spark.java;

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

import com.google.common.base.Optional;

import scala.Tuple2;
import scala.Tuple4;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("e03-joins")
								.setMaster("local[*]");


		try (JavaSparkContext jsc = new JavaSparkContext(config)) {

			/*
			 *  1: Total amount of money by ProductID as JavaPairRDD
			 */
			
			/* Load the Transactions File as a JavaRDD */
			Path transactionFilePath = Paths.get("./src/main/resources", "ch04_data_transactions.txt");
			JavaRDD<String> transactionFileLines = jsc.textFile(transactionFilePath.toString());
			JavaRDD<String[]> transactionFileFields = transactionFileLines.map(line -> line.split("#"));
			transactionFileFields.take(5).forEach(fieldsArray -> System.out.println(Arrays.asList(fieldsArray)));
			LOGGER.debug("Number of records in the transactions file: {}", transactionFileFields.count());
			printSeparator();
			

			JavaPairRDD<String,Double> totalMoneyByProductId = transactionFileFields
																	.mapToPair(trxFields -> new Tuple2<>(trxFields[3], Double.valueOf(trxFields[5])))
																	.reduceByKey((amount1, amount2) -> amount1 + amount2);
			System.out.println(totalMoneyByProductId.take(5));
			LOGGER.debug("Number of records in the moneyByProductRDD: {}", totalMoneyByProductId.count());
			
			/*
			 * 2: Total amount of money by Product Name as JavaPairRDD
			 */
			
			/* Load the Products File as a JavaRDD */
			Path productsFilePath = Paths.get("./src/main/resources", "ch04_data_products.txt");
			JavaRDD<String> productsFileLines = jsc.textFile(productsFilePath.toString());
			JavaRDD<String[]> productsFileFields = productsFileLines.map(line -> line.split("#"));
			productsFileFields.take(5).forEach(fieldsArray -> System.out.println(Arrays.asList(fieldsArray)));
			LOGGER.debug("Number of records in the products file: {}", productsFileFields.count());
			printSeparator();		
			
			/* Create the ProductName by ProductID JavaPairRDD */
			JavaPairRDD<String, String> productNameByProductId = productsFileFields.mapToPair(productFields -> new Tuple2<>(productFields[0], productFields[1]));
			System.out.println(productNameByProductId.take(5));
			printSeparator();
			
						
			/* Do the join */
			JavaPairRDD<String, Tuple2<String,Double>> totalsAndProductNameByProductId = productNameByProductId.join(totalMoneyByProductId);  
			System.out.println(totalsAndProductNameByProductId.take(5));
			System.out.println(totalsAndProductNameByProductId.lookup("84")); // -> Cyanocobalamin, 75192.53
			printSeparator();
			
			
			/*
			 * 3: List of products that were not sold
			 */
			
			/* Method 1: using leftOuterJoin */
			
			/* the left outer joining product with transactions will get empty records for products not sold */
			JavaPairRDD<String,Tuple2<String, Optional<Double>>> productNameAndTotalByProductId = productNameByProductId.leftOuterJoin(totalMoneyByProductId);
			
			/* then a filter can be applied for non-empty products */
			JavaPairRDD<String, String> productsNotSold = productNameAndTotalByProductId
															.filter(entry -> !entry._2()._2.isPresent())
															.mapValues(tuple2 -> tuple2._1());
			
			System.out.println(productsNotSold.take(5)); // only 4 were not sold (43, Tomb Raider PC), (63, Pajamas), (3, Cute baby doll), (20, LEGO Elves)
			printSeparator();
			
			/* Method 2: using subtractByKey */
			
			/* aliasing for readability purposes */
			JavaPairRDD<String, String> allProducts = productNameByProductId;
			JavaPairRDD<String, Double> productsSold = totalMoneyByProductId;
			
			JavaPairRDD<String,String> productsNotSoldAlt = allProducts.subtractByKey(productsSold);
			System.out.println(productsNotSoldAlt.take(5)); // only 4 were not sold (43, Tomb Raider PC), (63, Pajamas), (3, Cute baby doll), (20, LEGO Elves)
			printSeparator();
			
			/* Method 3: using cogroup */
			
			JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Double>>> cogroupByProductId = allProducts.cogroup(productsSold);
			System.out.println(cogroupByProductId.take(5));
			
			/* now we can filter out, when the total is empty */
			JavaPairRDD<String,Iterable<String>> productsNotSoldAlt2 = cogroupByProductId
																		.filter(tuple2 -> tuple2._2()._2().spliterator().getExactSizeIfKnown() == 0)
																		.mapValues(tuple2 -> tuple2._1()); 
																		
			System.out.println(productsNotSoldAlt2.take(5));
			printSeparator();
			
			/*
			 * 4: The sorted list of transactions with product names, sorted by product name
			 */
			
			/* Method 1: first convert JavaPairRDD to JavaRDD then apply sortBy */
			System.out.println(totalsAndProductNameByProductId.take(5));
			JavaRDD<Tuple2<String,Tuple2<String,Double>>> totalsAndProductNameByProductIdAsRDD =
					totalsAndProductNameByProductId.map(tuple2 -> tuple2);
			JavaRDD<Tuple2<String,Tuple2<String,Double>>> totalsAndProductNameByProductIdASortedByProductNameAsRDD = 
					totalsAndProductNameByProductIdAsRDD.sortBy(tuple2 -> tuple2._2()._1(), true, 4);
			System.out.println(totalsAndProductNameByProductIdASortedByProductNameAsRDD.take(5));
			printSeparator();
			
			/* Method 2: put the productname in the key and then apply sortByKey, and then back again */
			JavaPairRDD<String,Tuple2<String,Double>> productIdAndTotalsByProductName = totalsAndProductNameByProductId
					.mapToPair(tuple2 -> new Tuple2<>(tuple2._2()._1(), new Tuple2<>(tuple2._1(), tuple2._2()._2())));
			System.out.println(productIdAndTotalsByProductName.take(5));
			
			JavaPairRDD<String, Tuple2<String, Double>> productIdAndTotalsByProductNameSorted = productIdAndTotalsByProductName.sortByKey();
			System.out.println(productIdAndTotalsByProductNameSorted.take(5));
			
			JavaPairRDD<String, Tuple2<String, Double>> productNameAndTotalsByProductIdSortedByName =
					productIdAndTotalsByProductNameSorted.mapToPair(tuple2 -> new Tuple2<>(tuple2._2()._1(), new Tuple2<>(tuple2._1(), tuple2._2()._2())));
			
			System.out.println(productNameAndTotalsByProductIdSortedByName.take(5));
			printSeparator();
			
			/* Method 3: if we don't need to sort the whole data set */
			System.out.println(totalsAndProductNameByProductId.takeOrdered(5, byProductName()));
			printSeparator();
			
			/*
			 * 5: The average, max, min and total price of products bought per customer
			 */
			JavaPairRDD<String,String[]> transByCustomerId = transactionFileFields.mapToPair(trxFields -> new Tuple2<>(trxFields[2], trxFields));
			transByCustomerId.foreach(tuple2 -> System.out.println(tuple2._1() + "=>" + Arrays.asList(tuple2._2())));
			
			/* 
			 * this is done using combineByKey on a JavaPairRDD<K,V>, which in the most complete form is:
			 *  JavaPairRDD<K,C> combineByKey(
			 *  					Function<V,C> createCombiner,
			 *  					Function2<C,V,C> mergeValue,
			 *                      Function2<C,C,C> mergeCombiners,
			 *                      Partitioner partitioner,
			 *                      boolean mapSideCombine,
			 *                      Serializer serializer)
			 * 
			 *                      
			 * createCombiner function: create the first combined value of type C from the first key's value in each partition                     
			 * mergeValue function    : used to merge additional key values with the combined value, inside a single partition                     
			 * mergeCombiners function: used to merge combined values themselves among partitions
			 * 
			 * Partitioner : the partitioner, which if it is the same as the existing one, there will be no need for shuffling,
			 *               because all the elements with the same key will be in the same partition.
			 *               Note that if there's no shuffle, the mergeCombiners function won't be called at all.
			 *               
			 * mapSideCombine: whether to merge combined values within the partition before the shuffle.
			 * 
			 * serializer: lets you specify a custom serializer if you don't want to use the default one.
			 * 
			 * 
			 * Note that the Java API lets you use a version that assumes as Partitioner one that hash-partitions the result,
			 * using mapSideCombine=true and the default serializer.
			 * 
			 * The C class should be defined to hold the combined values, for example in this case, we'd need to define a
			 * Tuple4 to hold min, max, qty and total values. With the total and qty we can get the average
			 */
			
			/* createCombiner function: create the first combined value of type C from the first key's value in each partition */
			Function<String[],Tuple4<Double,Double,Integer,Double>> createCombiner = trxFields -> {
				Double firstMin = Double.valueOf(trxFields[5]);
				Double firstMax = firstMin;
				Integer firstQty = Integer.valueOf(trxFields[4]);
				Double firstTotal = Double.valueOf(trxFields[5]);
				
				return new Tuple4<>(firstMin / firstQty, firstMax / firstQty, firstQty, firstTotal);
			};
									
			/* mergeValue function    : used to merge additional key values with the combined value, inside a single partition */
			Function2<Tuple4<Double,Double,Integer,Double>,String[],Tuple4<Double,Double,Integer,Double>> mergeValue =
					(currentStats, trxFields) -> {
						Double mergedMin = Math.min(currentStats._1(), Double.valueOf(trxFields[5]));
						Double mergedMax = Math.max(currentStats._2(), Double.valueOf(trxFields[5]));
						Integer mergedQty = currentStats._3() + Integer.valueOf(trxFields[4]);
						Double mergedTotal = currentStats._4() + Double.valueOf(trxFields[5]);
						
						return new Tuple4<>(mergedMin, mergedMax, mergedQty, mergedTotal);
					};
					
					
			/* mergeCombiners function: used to merge combined values themselves among partitions                              */
			Function2<Tuple4<Double,Double,Integer,Double>,Tuple4<Double,Double,Integer,Double>,Tuple4<Double,Double,Integer,Double>> mergeCombiners =
				(partitionStats1, partitionStats2) -> {
					Double combinedMin = Math.min(partitionStats1._1(), partitionStats2._1());
					Double combinedMax = Math.max(partitionStats1._2(), partitionStats2._2());
					Integer combinedQty = partitionStats1._3() + partitionStats2._3();
					Double combinedTotal = partitionStats1._4() + partitionStats2._4();
					
					return new Tuple4<>(combinedMin, combinedMax, combinedQty, combinedTotal);
				};
				
			/* We define the partitioner ourselves to make sure no shuffling is performed */
			Partitioner partitioner = new HashPartitioner(transByCustomerId.partitions().size());	
			
			JavaPairRDD<String, Tuple4<Double, Double, Integer, Double>> statsByCustomerId =
					transByCustomerId.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner);
			
			printSeparator();
			statsByCustomerId.foreach(tuple2 -> System.out.println(tuple2._1() + "=>" + Arrays.asList(tuple2._2())));
			printSeparator();
			
		}		
	}
	
	
	private static void printSeparator() {
		System.out.println("=================================================================================");
	}	
	
	public static Comparator<Tuple2<String, Tuple2<String, Double>>> byProductName() {
		return (Comparator<Tuple2<String, Tuple2<String, Double>>> & Serializable) (tuple1, tuple2) -> tuple1._2()._1().compareTo(tuple2._2()._1());
	}	
}
