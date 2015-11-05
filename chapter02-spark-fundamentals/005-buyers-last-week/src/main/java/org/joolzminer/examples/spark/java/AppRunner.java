package org.joolzminer.examples.spark.java;

import java.nio.file.Paths;
import java.util.Arrays;
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
								.setAppName("005-buyers-last-week")
								.setMaster("local[*]");

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {		
			JavaRDD<String> dailyLines = sparkContext.textFile(Paths.get("./src/main/resources", "client-ids.log").toString());
			
			/* regular map gets us a collection of String[] */
			JavaRDD<String[]> dailyIds = dailyLines.map(dailyLine -> dailyLine.split(","));
			dailyIds.foreach(elem -> System.out.println(Arrays.asList(elem)));
			printSeparator();
			
			List<String[]> dailyIdStrList = dailyIds.collect();
			System.out.println("idStrs.size=" + dailyIdStrList.size());
			dailyIdStrList.forEach(elem -> System.out.println(Arrays.asList(elem)));
			printSeparator();
			
			/* flatmap gets us what we want: a collection of Strings */
			JavaRDD<String> strIdList = dailyLines.flatMap(line -> Arrays.asList(line.split(",")));
			JavaRDD<Integer> ids = strIdList.map(Integer::parseInt);
			JavaRDD<Integer> uniqueIds = ids.distinct();
			
			System.out.println("Summary:");
			System.out.println(ids.count() + " purchase(s) were recorded last week");
			System.out.println(uniqueIds.count() + " different client(s) performed purchases last week");
			printSeparator();
		}
	
	}	
	
	private static final void printSeparator() {
		System.out.println("================================================================");
	}
}
