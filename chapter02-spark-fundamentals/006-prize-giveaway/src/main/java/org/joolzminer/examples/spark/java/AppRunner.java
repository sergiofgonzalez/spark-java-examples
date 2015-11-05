package org.joolzminer.examples.spark.java;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("006-prize-giveaway")
								.setMaster("local[*]");

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {		
			JavaRDD<String> dailyLines = sparkContext.textFile(Paths.get("./src/main/resources", "client-ids.log").toString());
			
			/* flatmap gets us what we want: a collection of Strings */
			JavaRDD<String> strIdList = dailyLines.flatMap(line -> Arrays.asList(line.split(",")));
			JavaRDD<Integer> ids = strIdList.map(Integer::parseInt);
			JavaRDD<Integer> uniqueIds = ids.distinct();
			
			
			JavaRDD<Integer> sampleRDD = uniqueIds.sample(false, 0.07);
			List<Integer> sampleList = uniqueIds.takeSample(false, 5);
			System.out.println("Sampling:");
			System.out.println("sample, without replacement, 7% fraction:" + sampleRDD.collect());
			System.out.println("take sample, without replacement, 5 elements:" + sampleList);
			
			printSeparator();
			System.out.println("Take: " + uniqueIds.take(5));
			System.out.println("Min: " + uniqueIds.min(new Comparator<Integer>() {

				@Override
				public int compare(Integer o1, Integer o2) {
					// TODO Auto-generated method stub
					return 0;
				}}));
		}
	
	}	
	
	private static final void printSeparator() {
		System.out.println("================================================================");
	}
}
