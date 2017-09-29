package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
import java.util.function.BiConsumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("009-hello-rdd-creation")
						.withPropertiesFile("config/spark-job.conf")
						.withExtraConfigVars(args)
						.doing(createRdd)
						.submit());		
	}
	
	

	

	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> createRdd = (spark, sparkContext) -> {
		/* Use parallelize to create an RDD from a materialized List */
		JavaRDD<String> strings = sparkContext.parallelize(Arrays.asList("uno", "dos", "tres", "catorce"));
		LOGGER.info("The created RDD contains {} elements", strings.count());
		
		/* textFile method also provides an RDD */
		JavaRDD<String> lines = sparkContext.textFile("./src/main/resources/log4j.properties");
		LOGGER.info("The RDD created from the file contains {} lines", lines.count());
	};

}
