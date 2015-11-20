package org.joolzminer.examples.spark.java;

import org.apache.spark.Accumulable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joolzminer.examples.spark.java.accumulables.AverageAccumulableFunctionHolder;
import org.joolzminer.examples.spark.java.accumulables.utils.AccumulableParamAdapter;
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
								.setAppName("010-accumulators")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			
			AccumulableParamAdapter<Integer, Tuple2<Integer, Integer>> avgAccumulableParam = 
					new AccumulableParamAdapter<>(new AverageAccumulableFunctionHolder());
			
			JavaRDD<Integer> rdd = sc.parallelize(IntListBuilder.getInts().from(1).to(1_000_000).build());
			
			Accumulable<Tuple2<Integer, Integer>, Integer> accumulable = sc.accumulable(new Tuple2<>(0, 0), avgAccumulableParam);
			rdd.foreach(item -> accumulable.add(item));
			
			double mean = ((double)accumulable.value()._2()) / accumulable.value()._1();
			System.out.println();
			System.out.println("mean=" + mean);
		}
			
	}
	
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
