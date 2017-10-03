package com.github.sergiofgonzalez.examples.spark.java;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;
import java.io.File;

import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;

import java.util.ArrayList;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("015-hello-save-rdd")
						.withPropertiesFile("config/application.conf")
						.withExtraConfigVars(args)
						.doing(savingRddApp)
						.submit());		
	}
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> savingRddApp = (spark, sparkContext) -> {
		/* There is no saveMode overwrite for saveTextFile so we use FileUtils to delete locations */
		FileUtils.deleteQuietly(new File(wconf().get("output_data_partitioned")));
		FileUtils.deleteQuietly(new File(wconf().get("output_data_single_list")));
		FileUtils.deleteQuietly(new File(wconf().get("output_data_single_array")));
		FileUtils.deleteQuietly(new File(wconf().get("output_data_single_custom")));
		
		/* Writing a JavaRDD<List<String>> */
		JavaRDD<List<String>> actorsListRDD = sparkContext.parallelize(Arrays.asList(
				Arrays.asList("Idris", "Elba", "1972"),
				Arrays.asList("Jason","Isaacs","1963"),
				Arrays.asList("Riz","Ahmed","1982"),
				Arrays.asList("Emma","Watson","1990")
				));		
		
		/* This creates several partitions, which is good for computer processing and bad for humans */
		actorsListRDD
			.saveAsTextFile(wconf().get("output_data_partitioned"));
		
		/* you can create a single file using coalesce(1) */
		actorsListRDD
		.coalesce(1)
		.saveAsTextFile(wconf().get("output_data_single_list"));
		
		/* Previous technique is not safe, because toString is not smart enough to print contents (as in Arrays!)*/
		List<String[]> data = new ArrayList<>();
		data.add(new String[]{"Idris", "Elba", "1972"});
		data.add(new String[]{"Jason","Isaacs","1963"});
		data.add(new String[]{"Riz","Ahmed","1982"});
		data.add(new String[]{"Emma","Watson","1990"});
		
		JavaRDD<String[]> actorsArrayRDD = sparkContext.parallelize(data);		
		
		actorsArrayRDD
			.coalesce(1)
			.saveAsTextFile(wconf().get("output_data_single_array"));
		
		
		/* So it's recommended to do your own encoding */
		actorsArrayRDD
			.map(fields -> String.join(".", fields))
			.coalesce(1)
			.saveAsTextFile(wconf().get("output_data_single_custom"));		
	};
	
}
