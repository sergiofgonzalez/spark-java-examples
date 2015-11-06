package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joolzminer.examples.spark.java.utils.IntListBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("001-pair-rdds-java")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			/* You can create a `JavaPairRDD` with `JavaSparkContext.parallelizePairs` */
			JavaPairRDD<String, Integer> alphabetPairRDD = sc.parallelizePairs(Arrays.asList(
														  new Tuple2<String, Integer>("a", 1),
														  new Tuple2<String, Integer>("b", 2),
														  new Tuple2<String, Integer>("c", 3)));
			
			System.out.println("The PairRDD have " + alphabetPairRDD.count() + " element(s)");
			
			/* 
			 * You can use a `mapToPair` transformation on a `JavaRDDLike` and pass it a PairFunction
			 * that will be used to map each RDD's element into Tuple2<K,V> objects. 
			 */
			
			
			JavaRDD<Integer> rdd = sc.parallelize(IntListBuilder
													.getInts()
													.from(1)
													.to(5)
													.build());
			
			JavaPairRDD<Integer, Boolean> pairs = rdd.mapToPair(num -> new Tuple2<>(num, false));
			
			System.out.println("The pairs have " + pairs.count() + " element(s)");
		}

	}
}
