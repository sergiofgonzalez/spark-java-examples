package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("e04-join-sandbox")
								.setMaster("local[*]");


		try (JavaSparkContext jsc = new JavaSparkContext(config)) {

			/* Super basic join example */
			JavaPairRDD<Integer, String> numsInSpanish = jsc.parallelizePairs(Arrays.asList(
					new Tuple2<>(1, "uno"),
					new Tuple2<>(2, "dos"),
					new Tuple2<>(3, "tres"),
					new Tuple2<>(2, "dos")));
			
			JavaPairRDD<Integer, String> numsInEnglish = jsc.parallelizePairs(Arrays.asList(
					new Tuple2<>(1, "one"),
					new Tuple2<>(2, "two")));
			
			System.out.println("== join");
			JavaPairRDD<Integer, Tuple2<String, String>> numsDictionaryJoin = numsInSpanish.join(numsInEnglish);
			numsDictionaryJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== left outer join");
			JavaPairRDD<Integer, Tuple2<String, Optional<String>>> numsDictionaryLeftOuterJoin = numsInSpanish.leftOuterJoin(numsInEnglish);
			numsDictionaryLeftOuterJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== right outer join");
			JavaPairRDD<Integer, Tuple2<Optional<String>, String>> numsDictionaryRightOuterJoin = numsInSpanish.rightOuterJoin(numsInEnglish);
			numsDictionaryRightOuterJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== full outer join");
			JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<String>>> numsDictionaryFullOuterJoin = numsInSpanish.fullOuterJoin(numsInEnglish);
			numsDictionaryFullOuterJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== join (inverse)");
			JavaPairRDD<Integer, Tuple2<String, String>> numsDictionaryInverseJoin = numsInEnglish.join(numsInSpanish);
			numsDictionaryInverseJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== left outer join (inverse)");
			JavaPairRDD<Integer, Tuple2<String, Optional<String>>> numsDictionaryInverseLeftOuterJoin = numsInEnglish.leftOuterJoin(numsInSpanish);
			numsDictionaryInverseLeftOuterJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== right outer join (inverse)");
			JavaPairRDD<Integer, Tuple2<Optional<String>, String>> numsDictionaryInverseRightOuterJoin = numsInEnglish.rightOuterJoin(numsInSpanish);
			numsDictionaryInverseRightOuterJoin.foreach(System.out::println);
			printSeparator();
			
			System.out.println("== full outer join (inverse)");
			JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<String>>> numsDictionaryInverseFullOuterJoin = numsInEnglish.fullOuterJoin(numsInSpanish);
			numsDictionaryInverseFullOuterJoin.foreach(System.out::println);
			printSeparator();
		}		
	}
	
	
	private static void printSeparator() {
		System.out.println("=================================================================================");
	}	
}
