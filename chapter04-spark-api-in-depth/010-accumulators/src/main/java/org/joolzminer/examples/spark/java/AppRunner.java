package org.joolzminer.examples.spark.java;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joolzminer.examples.spark.java.utils.IntListBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("010-accumulators")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			/* Accumulator */
			Accumulator<Integer> accumulator = sc.accumulator(0, "my-accumulator");
			JavaRDD<Integer> list = sc.parallelize(IntListBuilder.getInts().from(1).to(1_000_000).build()); 
			
			list.foreach(i -> accumulator.add(1));
			
			System.out.println("Querying value from the driver: " + accumulator.value());
			
			try {
				list.foreach(i -> System.out.println("From the worker: " + accumulator.value()));
			} catch (Throwable t) {
				System.out.println("Exception caught: " + t.getMessage());
			}
			printSeparator();
			
			
			/* Accumulator for Lists */
			Accumulator<List<String[]>> strAccumulator = sc.accumulator(new ArrayList<String[]>(), "strAccumulator", new RecordHolder());
			JavaRDD<Integer> smallList = sc.parallelize(IntListBuilder.getInts().from(1).to(5).build()); 
			smallList.foreach(i -> strAccumulator.add(new ArrayList<String[]>() {{
					add(new String[] {"one", "two", "three"}); 
			}}));
			
			strAccumulator.value().forEach(fields -> System.out.println(Arrays.asList(fields)));
			
			System.out.println("Querying value from the driver: " + accumulator.value());
			
			try {
				list.foreach(i -> System.out.println("From the worker: " + accumulator.value()));
			} catch (Throwable t) {
				System.out.println("Exception caught: " + t.getMessage());
			}
			printSeparator();
		}
			
	}
	
	
	static class RecordHolder implements AccumulatorParam<List<String[]>> {

		@Override
		public List<String[]> addInPlace(List<String[]> list1, List<String[]> list2) {
			list1.addAll(list2);
			return list1;
		}

		@Override
		public List<String[]> zero(List<String[]> list) {
			return list;
		}

		@Override
		public List<String[]> addAccumulator(List<String[]> list1, List<String[]> list2) {
			list1.addAll(list2);
			return list1;
		}


	}
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
