package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;
import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;
import static java.util.Comparator.*;
import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("CountWordsApp")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(countWordsFn)
						.submit());		
	}
	
		
	private static final Comparator<Entry<String, Long>> compareByValue = comparing(Entry::getValue);
	private static final Comparator<Entry<String, Long>> compareByValueReversed = compareByValue.reversed();
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> countWordsFn = (spark, sparkContext) -> {
		
		/* Load the file */
		Path localInputFilePath = downloadInputFileToLocalPath();
		JavaRDD<String> linesRDD = sparkContext.textFile(localInputFilePath.toString());
		LOGGER.info("Num lines in downloaded text: {}", linesRDD.count());
		
		/* Split each line by space */
		JavaRDD<String> wordsRDD = linesRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		
		/* Normalize to lowercase, cleaning punctuation marks */
		String textLocaleTag = wconf().get("files.text_locale");
		String allowedCharsRegex = wconf().get("files.allowed_chars_regex");
		Pattern allowedCharsPattern = Pattern.compile(allowedCharsRegex);
		wordsRDD = wordsRDD
					.map(word -> word.toLowerCase(Locale.forLanguageTag(textLocaleTag)))
					.map(word -> allowedCharsPattern.matcher(word).replaceAll(""));
		
		JavaPairRDD<String,Integer> wordOccurrencesRDD = wordsRDD.mapToPair(word -> new Tuple2<String,Integer>(word, 1));
						
		/* Materialize in Java Map */
		Map<String,Long> wordOccurrences = wordOccurrencesRDD.countByKey();
		
		/* Perform the sorting in Java */
		Map<String,Long> sortedWordOccurrences = wordOccurrences.entrySet().stream()
													.sorted(compareByValueReversed)
													.collect(toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

		/* Display the results on the console */
		sortedWordOccurrences.entrySet().stream()
			.forEach(entry -> LOGGER.info("{}={}", entry.getKey(), entry.getValue()));		
	};
	
	
	private static final URL getInputFileAsUrl() {
		String inputFile = wconf().get("files.input");
		
		try {
			return new URL(inputFile);
		} catch (MalformedURLException e) {
			LOGGER.error("The input file {} could not be wrapped into a URL", inputFile, e);
			throw new IllegalArgumentException("The configured input file could not be wrapped into a URL", e);
		}
	}
	
	private static final Path downloadInputFileToLocalPath() {
		URL inputFileUrl = getInputFileAsUrl(); 
		Path localDownloadFilePath = Paths.get(wconf().get("files.local_download_path"), inputFileUrl.getFile());
		try {
			FileUtils.copyURLToFile(getInputFileAsUrl(), localDownloadFilePath.toFile());
		} catch (IOException e) {
			LOGGER.error("Could not download {} -> {}", inputFileUrl.toString(), localDownloadFilePath.toString(), e);
			throw new IllegalArgumentException("Could not download input file", e);
		}
		
		return localDownloadFilePath;
	}
}
