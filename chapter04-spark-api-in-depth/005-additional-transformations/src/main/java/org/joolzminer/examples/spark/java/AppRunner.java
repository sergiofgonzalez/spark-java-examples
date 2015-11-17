package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("005-additional-transformations")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			/*
			 * intersection
			 */
			JavaRDD<Integer> oddNumbersRDD = sc.parallelize(Arrays.asList(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
			JavaRDD<Integer> primeNumbersRDD = sc.parallelize(Arrays.asList(2, 3, 5, 7, 11, 13, 17, 19));
			
			JavaRDD<Integer> oddPrimeIntersection = oddNumbersRDD.intersection(primeNumbersRDD);
			oddPrimeIntersection.foreach(System.out::println);
			printSeparator();
			
			
			/*
			 * cartesian product a: find the pairs that are divisible, that is (n1, n2) that n1 is divisible by n2
			 */
			JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(7, 8, 9));
			JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));
			JavaPairRDD<Integer, Integer> cartesianProduct = rdd1.cartesian(rdd2);
			cartesianProduct.foreach(System.out::println);
			printSeparator();
			JavaPairRDD<Integer, Integer> divisiblePairsRDD = cartesianProduct.filter(p -> (p._1() % p._2()) == 0);
			divisiblePairsRDD.foreach(System.out::println);
			printSeparator();
			
			/*
			 * cartesian product a: cartesian product of two RDDs with different element types
			 */
			JavaRDD<Character> charRDD = sc.parallelize(Arrays.asList('a', 'b', 'c'));
			JavaRDD<Integer> numRDD = sc.parallelize(Arrays.asList(1, 2, 3)); 
			JavaPairRDD<Character, Integer> charNumCartesianProductRDD = charRDD.cartesian(numRDD);
			charNumCartesianProductRDD.foreach(System.out::println);
			printSeparator();
			
			/*
			 * zip
			 */
			JavaRDD<Character> vowelsRDD = sc.parallelize(Arrays.asList('a', 'e', 'i', 'o', 'u'));
			JavaRDD<Integer> someNumsRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
			JavaPairRDD<Character, Integer> zippedResult = vowelsRDD.zip(someNumsRDD);
			zippedResult.foreach(System.out::println);
			printSeparator();
			
			/*
			 * zipPartitions
			 */
			JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
			JavaRDD<String> numStrs = sc.parallelize(Arrays.asList("1", "2", "3", "4"), 2);
			FlatMapFunction2<Iterator<Integer>, Iterator<String>, Integer> sizesFn = 
					(Iterator<Integer> iterNum, Iterator<String> iterStr) -> {
						int sizeI = 0;
						while (iterNum.hasNext()) {
							sizeI += 1;
							iterNum.next();
						}
						int sizeS = 0;
						while (iterStr.hasNext()) {
							sizeS += 1;
							iterStr.next();
						}
						return Arrays.asList(sizeI, sizeS);
					};
			JavaRDD<Integer> sizes = nums.zipPartitions(numStrs, sizesFn);
			sizes.foreach(System.out::println);
			printSeparator();
		}
	}


	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
