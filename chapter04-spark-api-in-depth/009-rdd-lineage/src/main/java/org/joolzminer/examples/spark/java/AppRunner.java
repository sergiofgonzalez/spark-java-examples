package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import java.io.IOException;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("009-rdd-lineage")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			/* create local list of random nums from 0 to 9 */
			List<Integer> list = fill(500, 10);									

			/* parallelize the list across 5 partitions */
			JavaRDD<Integer> listrdd = sc.parallelize(list, 5);
			
			/* create a pair RDD */
			JavaPairRDD<Integer, Integer> pairs = listrdd.mapToPair(x -> new Tuple2<>(x, x * x));

			/* reduce by key: causes shuffling */
			JavaPairRDD<Integer, Integer> reduced = pairs.reduceByKey((v1, v2) -> v1 + v2);

			/* map within partition */
			JavaRDD<String> finalrdd = reduced.mapPartitions(iter -> {
				List<String> result = new ArrayList<>();
				while (iter.hasNext()) {
					Tuple2<Integer, Integer> tuple2 = iter.next();
					String item = "{K=" + tuple2._1() + ", V=" + tuple2._2() + "}";
					result.add(item);
				}
				return result;
			});
			
			/* collect results in the driver */
			List<String> resultList = finalrdd.collect();
			
			System.out.println();
			System.out.println(resultList);			
			printSeparator();

			System.out.println(finalrdd.toDebugString());
			printSeparator();
		}
			
	}

	private static List<Integer> fill(int size, int bound) {
		List<Integer> list = new ArrayList<>(size);
		Random rand = new Random();
		for (int i = 0; i < size; i++) {
			list.add(rand.nextInt(bound));
		}
		return list;
	}
	
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
