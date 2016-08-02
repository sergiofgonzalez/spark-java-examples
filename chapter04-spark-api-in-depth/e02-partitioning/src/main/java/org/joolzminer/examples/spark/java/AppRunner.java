package org.joolzminer.examples.spark.java;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("e02-partitioning")
								.setMaster("local[*]");


		try (JavaSparkContext jsc = new JavaSparkContext(config)) {
			Path dataFilePath = Paths.get("./src/main/resources", "sample-data.csv");
			JavaRDD<String> dataFileLinesJavaRDD = jsc.textFile(dataFilePath.toString());
			LOGGER.debug("dataFileLines.count={}", dataFileLinesJavaRDD.count());
			System.out.println("Displaying the first 5 lines of the RDD:");
			dataFileLinesJavaRDD.take(5).stream().forEach(System.out::println);
			printSeparator();
			
			JavaRDD<String[]> dataFieldsJavaRDD = dataFileLinesJavaRDD.map(line -> line.split(","));
			
			JavaPairRDD<String,String[]> dataByLocationJavaPairRDD = dataFieldsJavaRDD.mapToPair(fields -> new Tuple2<>(fields[0], fields));
			System.out.println("Displaying the first 5 lines of the Pair RDD:");
			dataByLocationJavaPairRDD.take(5).stream().forEach(tuple2 -> System.out.println(tuple2._1() + "=>" + Arrays.asList(tuple2._2())));
			printSeparator();
			
			dataByLocationJavaPairRDD
				.glom()
				.foreach(System.out::println);
			printSeparator();

			/* Let's do something useful such as sum by site */
			
			Partitioner customPartitioner = PartitionerBySite
														.create(String.class)
														.from(dataByLocationJavaPairRDD)
														.build();
			
			JavaPairRDD<String,String[]> dataPartitionedByLocationJavaPairRDD = dataByLocationJavaPairRDD
																					.partitionBy(customPartitioner);
																			
			
			dataPartitionedByLocationJavaPairRDD.glom().foreach(System.out::println);
			printSeparator();
			
			JavaPairRDD<String,Double> levelByLocationJavaPairRDD = dataByLocationJavaPairRDD
																		.mapPartitionsToPair(inPartitionFn);
			levelByLocationJavaPairRDD.glom().foreach(System.out::println);
			

			Map<String, Double> sumBySiteMap = levelByLocationJavaPairRDD.reduceByKeyLocally((value1, value2) -> value1 + value2);
			sumBySiteMap.entrySet().stream().forEach(entry -> System.out.println(entry.getKey() + "=>" + entry.getValue()));
			
		}		
	}
	
	
	private static PairFlatMapFunction<Iterator<Tuple2<String,String[]>>, String, Double> inPartitionFn = input -> {
		List<Tuple2<String, Double>> out = new LinkedList<>();
		while (input.hasNext()) {
			Tuple2<String, String[]> inputTuple2 = input.next();
			Tuple2<String,Double> outTuple2 = new Tuple2<>(inputTuple2._1(), Double.valueOf(inputTuple2._2()[2]));
			out.add(outTuple2);
		}
		return out;
	};
	
	private static void printSeparator() {
		System.out.println("=================================================================================");
	}	
}
