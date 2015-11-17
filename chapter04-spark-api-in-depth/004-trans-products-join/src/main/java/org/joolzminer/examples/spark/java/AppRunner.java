package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;

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
			 * Step 5: Create a pair RDD with the totals by product
			 */
			JavaPairRDD<String, Double> totalsByProductPairRDD = amountByProductPairRDD.reduceByKey((amt1, amt2) -> amt1 + amt2);
			totalsByProductPairRDD.foreach(AppRunner::prettyPrintAmount);
			printSeparator();
			
			/*
			 * Step 6: Obtain the pair RDD resulting of joining totals by product and products.
			 */
			JavaPairRDD<String, Tuple2<Double, String[]>> productTotalsAndAttributesPairRDD =
					totalsByProductPairRDD.join(productsPairRDD);
			productTotalsAndAttributesPairRDD.foreach(AppRunner::prettyPrintTotalsAndAttributes);
			printSeparator();
			
			/*
			 * Step 7.a: Obtain the products that were not sold yesterday: using leftOuterJoin with products
			 */
			JavaPairRDD<String,Tuple2<String[], Optional<Double>>> totalsWithMissingProductsPairRDD =
					productsPairRDD.leftOuterJoin(totalsByProductPairRDD);
			
			JavaRDD<String[]> missingProdsRDD = totalsWithMissingProductsPairRDD
													.filter(tuple2 -> !tuple2._2()._2().isPresent())
													.map(tuple2 -> tuple2._2()._1());
															
			missingProdsRDD.foreach(item -> System.out.println(Arrays.asList(item)));
			printSeparator();		
			
			/*
			 * Step 7.b: Obtain the products that were not sold yesterday: using rightOuterJoin with totals by product
			 */
			JavaPairRDD<String,Tuple2<Optional<Double>, String[]>> totalsWithMissingProductsPairRDDAlt =
					totalsByProductPairRDD.rightOuterJoin(productsPairRDD);
			
			JavaRDD<String[]> missingProdsRDDAlt = totalsWithMissingProductsPairRDDAlt
													.filter(tuple2 -> !tuple2._2()._1().isPresent())
													.map(tuple2 -> tuple2._2()._2());
															
			missingProdsRDDAlt.foreach(item -> System.out.println(Arrays.asList(item)));
			printSeparator();
			
			/*
			 * Step 7.c: Obtain the products that were not sold yesterday: using subtractByKey with totals by product
			 */
			JavaPairRDD<String,String[]> missingProductsPairRDDAltAlt =
					productsPairRDD.subtractByKey(totalsByProductPairRDD);
			
															
			missingProductsPairRDDAltAlt.foreach(tuple -> System.out.println(Arrays.asList(tuple._2())));
			printSeparator();
			
			/*
			 * Step 7.d: Obtain the products that were not sold yesterday: using cogroup
			 */
			JavaPairRDD<String, Tuple2<Iterable<Double>, Iterable<String[]>>> productTotalsCogroupRDD = 
					totalsByProductPairRDD.cogroup(productsPairRDD);
			
			JavaPairRDD<String, Tuple2<Iterable<Double>,Iterable<String[]>>> missingProductTotalsRDD = 
							productTotalsCogroupRDD
									.filter(tuple2 -> !tuple2._2()._1().iterator().hasNext());
			
			JavaRDD<Iterable<String[]>> missingProductsRDDAlt = missingProductTotalsRDD.map(tuple2 -> tuple2._2()._2());
															
			missingProductsRDDAlt.foreach(item -> item
					.iterator()
					.forEachRemaining(productFields -> System.out.println(Arrays.asList(productFields))));
			printSeparator();
			
		}
	}

	
	private static void prettyPrint(Tuple2<String, String[]> trxTuple) {
		System.out.println(trxTuple._1() + "\t:" + Arrays.asList( trxTuple._2()));
	}
	
	private static void prettyPrintAmount(Tuple2<String, Double> trxTuple) {
		System.out.println(trxTuple._1() + "\t: $" + trxTuple._2());
	}	
	
	private  static void prettyPrintTotalsAndAttributes(Tuple2<String, Tuple2<Double, String[]>> item) {
		System.out.println(item._1() + "\t: $" + item._2()._1() + "\t" + Arrays.asList(item._2()._2()));
	}
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
