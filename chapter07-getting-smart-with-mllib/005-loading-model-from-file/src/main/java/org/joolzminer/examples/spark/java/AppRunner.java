package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		
		SparkConf config = new SparkConf()
				.setAppName("005-loading-model-from-file")
				.setMaster("local[*]");
		
		try (JavaSparkContext sc = new JavaSparkContext(config)) {
						
			/*
			 * Loading a previously saved model from file
			 */
			String datasetPath = "./src/main/resources/housing-dataset";
			String modelPath = "housing-linear-regression-model";
			
			LinearRegressionModel savedModel = LinearRegressionModel.load(sc.sc(), Paths.get(datasetPath, modelPath).toString());
			
			double[] weights = savedModel.weights().toArray();
			for (int i = 0; i < weights.length; i++) {
				System.out.println("Feature #" + i + ": " + weights[i] * weights[i]);
			}
			printSeparator();
		}														
	}
	

	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}	
}
