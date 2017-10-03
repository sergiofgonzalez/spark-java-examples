package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;

import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("011-hello-parsing-delimited-file")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(parsingDelimitedFileApp)
						.submit());		
	}
	
	

	
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> parsingDelimitedFileApp = (spark, sparkContext) -> {
		JavaRDD<String> linesRDD = sparkContext.textFile(wconf().get("input_data"));
		JavaRDD<String[]> fieldsRDD = linesRDD.map(line -> line.split(wconf().get("delimiter")));
		JavaRDD<List<String>> fieldListRDD = fieldsRDD.map(fieldsArray -> Arrays.asList(fieldsArray));
		
		LOGGER.info("File parsed: {}", fieldListRDD.collect());
	};
}
