package org.joolzminer.examples.spark.java;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("006-prize-giveaway")
								.setMaster("local[*]");

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {		
			Path licFilePath = Paths.get(System.getenv("SPARK_HOME"), "LICENSE");
			JavaRDD<String> licLines = sparkContext.textFile(licFilePath.toString());
			JavaRDD<String> licWords = licLines.flatMap(line -> Arrays.asList(line.split(" ")));
			JavaRDD<Integer> licWordLengths = licWords.map(word -> word.length());
			
			int maxWordLength = licWordLengths.max(Comparator.<Integer>naturalOrder());			
		
			
			JavaRDD<Tuple2<String, Integer>> wordOccurrenceRDD = licWords.map(word -> new Tuple2<String, Integer>(word, 1));
			JavaPairRDD<String, Integer> wordOccurrencePairRDD = sparkContext.parallelizePairs(wordOccurrenceRDD.collect());
			Map<String, Object> wordOccurrenceMap = wordOccurrencePairRDD.countByKey();
			Stream<Entry<String, Object>> sortedWordOccurrenceMap = wordOccurrenceMap.entrySet()
																			.stream()
																			.sorted(compareByValue);
																									
			
			JavaRDD<Tuple2<String, Integer>> wordLengthRDD = licWords.map(word -> new Tuple2<String, Integer>(word, word.length()));
			JavaRDD<Tuple2<String, Integer>> sortedWordLengthRDD = wordLengthRDD.sortBy(Tuple2::_2, true, 1);

			
			
			
			System.out.println("The longest word of the file is " + maxWordLength + " characters long.");

			sortedWordOccurrenceMap.forEach(entry -> {
				System.out.println("'" + entry.getKey() + "' : " + entry.getValue());
			});
			
			printSeparator();
			sortedWordLengthRDD.foreach(tuple2 -> { if (tuple2._2() > 0) System.out.println(tuple2);});
		}			
	}	
	
	private static Comparator<Entry<String, Object>> compareByValue = Comparator.comparingLong(entry -> (Long) entry.getValue());
	
	private static final void printSeparator() {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	}
}
