package org.joolzminer.examples.spark.java;

import java.nio.file.Path;
import java.nio.file.Paths;

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
								.setAppName("001-hello-spark")
								.setMaster("local[*]");
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {						
			Path sparkLicenseFile = Paths.get(System.getenv("SPARK_HOME"), "LICENSE");			
			JavaRDD<String> licLines = sparkContext.textFile(sparkLicenseFile.toString());			
			System.out.println("Lines in Apache Spark License file: " + licLines.count());
		}
	}	
}
