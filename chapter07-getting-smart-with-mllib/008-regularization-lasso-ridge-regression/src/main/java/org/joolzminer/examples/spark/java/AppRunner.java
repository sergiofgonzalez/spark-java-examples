package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LassoModel;
import org.apache.spark.mllib.regression.LassoWithSGD;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.regression.RidgeRegressionModel;
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		
		SparkConf config = new SparkConf()
				.setAppName("008-regularization-lasso-ridge-regression")
				.setMaster("local[*]");
		
		try (JavaSparkContext sc = new JavaSparkContext(config)) {
						
			/*
			 * __Phase 0: Acquiring data__
			 * 
			 * Step 0: load the dataset lines
			 */
			String datasetPath = "./src/main/resources/housing-dataset";
			String datasetFilename = "housing.data";
			
			JavaRDD<String> housingDatasetLines = sc.textFile(Paths.get(datasetPath, datasetFilename).toString(), 6);
			long numLines = housingDatasetLines.count();
			System.out.println("\nLoaded " + numLines + " line(s) from " + datasetFilename);
			printSeparator();
			
			/*
			 * Step 1: obtain the values in each row and put them in a RDD of Vectors
			 */
			JavaRDD<double[]> rowFieldsArrays = housingDatasetLines
													.map(line -> Arrays.stream(line.trim().split("\\s+"))   // this is Spark's map
																				.mapToDouble(Double::parseDouble) // this is Java's map
																				.toArray());
			
			JavaRDD<Vector> housingValues = rowFieldsArrays.map(array -> Vectors.dense(array));
			long numVectors = housingValues.count();
			System.out.println("\nLoaded " + numVectors + " vectors(s) of data");
			printSeparator();
			
			housingValues.foreach(System.out::println);
			printSeparator();
						
			
			/*
			 * __Phase 2: Preparation of the data for Spark__
			 * 
			 * Step 1: Creating the `LabeledPoint` structure, which separates
			 * the target value (label) from the features.
			 * 
			 */
			JavaRDD<LabeledPoint> housingData = housingValues.map(vector -> {
				double[] vectorAsArray = vector.toArray();
				int vectorLen = vectorAsArray.length;
				
				return new LabeledPoint(vectorAsArray[vectorLen - 1], Vectors.dense(Arrays.copyOfRange(vectorAsArray, 0, vectorLen - 1)));	
			});			
			
			
			/*
			 * Adding higher-order polynomials
			 */
			
			JavaRDD<LabeledPoint> nonLinearHousingData = housingData.map(labeledPoint -> new LabeledPoint(labeledPoint.label(), getVectorWithHigherOrderPolynomials(labeledPoint.features())));
			int numFeatures = nonLinearHousingData.first().features().size();
			long numRows = nonLinearHousingData.count();
			
			printSeparator();
			prettyPrintDataset(nonLinearHousingData);
			printSeparator();
			prettyPrintDataset(housingData);
			printSeparator();
			System.out.println("Number of features on the non-linear dataset: " + numFeatures);
			System.out.println("Number of rows on the non-linear dataset    :" + numRows);
			printSeparator();
			
						
			/*
			 * Step 2: Splitting the data into training and testing data sets
			 */
			JavaRDD<LabeledPoint>[] dataSets = nonLinearHousingData.randomSplit(new double[] {0.8, 0.2});
			JavaRDD<LabeledPoint> housingDatasetTraining = dataSets[0];
			JavaRDD<LabeledPoint> housingDatasetTesting = dataSets[1];
			
			long numRowsTraining = housingDatasetTraining.count();
			long numRowsTesting = housingDatasetTesting.count();
			System.out.println("Original data set splitted using 80%-20% ratios: training=" + numRowsTraining + " row(s), testing=" + numRowsTesting + " row(s)");
			
			/*
			 * Step 3: Scaling the data using feature scaling and mean normalization
			 * 
			 * + feature scaling: ranges of data are scaled to comparable sizes
			 * + mean normalization: data is translated so that averages are roughly zero
			 * 
			 */
			StandardScaler scaler = new StandardScaler(true, true);
			StandardScalerModel fittedScaler = scaler.fit(housingDatasetTraining.map(row -> row.features()).rdd());
			
			JavaRDD<LabeledPoint> housingDataSetTrainingScaled = housingDatasetTraining
					.map(labeledPt -> new LabeledPoint(labeledPt.label(),  fittedScaler.transform(labeledPt.features())));
			
			housingDataSetTrainingScaled.cache();
			
			JavaRDD<LabeledPoint> housingDataSetTestingScaled = housingDatasetTesting
					.map(labeledPt -> new LabeledPoint(labeledPt.label(),  fittedScaler.transform(labeledPt.features())));
			housingDataSetTestingScaled.cache();

			
			/*
			 * Optimizing the model parameters using LinearRegression 
			 */

			iterateLinearRegressionWithSGD(
					Arrays.asList(200, 400), 
					Arrays.asList(0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5), 
					housingDataSetTrainingScaled, 
					housingDataSetTestingScaled);
			printSeparator();
			
			iterateLinearRegressionWithSGD(
					Arrays.asList(200, 400, 800, 1000, 3000, 6000), 
					Arrays.asList(1.2), 
					housingDataSetTrainingScaled, 
					housingDataSetTestingScaled);
			printSeparator();
			
			iterateLinearRegressionWithSGD(
					Arrays.asList(10_000, 15_000, 30_000, 50_000), 
					Arrays.asList(1.2), 
					housingDataSetTrainingScaled, 
					housingDataSetTestingScaled);
			printSeparator();

			/*
			 * Using Lasso and Ridge Regression to avoid overfitting to the training set
			 */
			System.out.println("Using Lasso:");
			iterateLassoWithSGD(
					Arrays.asList(200, 400, 1000, 3000, 6000, 10_000, 50_000, 200_000, 300_000), 
					Arrays.asList(1.1), 
					Arrays.asList(0.01), 
					housingDataSetTrainingScaled,
					housingDataSetTestingScaled);
			printSeparator();
			
			System.out.println("Using Ridge Regression:");
			iterateRidgeRegressionWithSGD(
					Arrays.asList(200, 400, 1000, 3000, 6000, 10_000, 50_000, 200_000, 300_000), 
					Arrays.asList(1.1), 
					Arrays.asList(0.01), 
					housingDataSetTrainingScaled,
					housingDataSetTestingScaled);
			printSeparator();			
		}														
	}
	
	
	private static void iterateLassoWithSGD(List<Integer> numIterationsList, List<Double> stepSizes, List<Double> regParams, JavaRDD<LabeledPoint> trainingDataset, JavaRDD<LabeledPoint> testingDataset) {
		StringBuffer sb = new StringBuffer();
		sb.append("numIters, stepSize, regParam, RMSE (training), RMSE(testing)\n");
		
		numIterationsList.stream()
			.forEach(numIterations -> stepSizes.stream().forEach(stepSize -> regParams.stream().forEach(regParam -> {
				LOGGER.debug("Creating Lasso model for numIterations={}, stepSize={}, regularizationParameter={}", numIterations, stepSize, regParam);
				LassoWithSGD algorithm = new LassoWithSGD();
				algorithm.setIntercept(true);
				algorithm
					.optimizer()
						.setNumIterations(numIterations)
						.setStepSize(stepSize)
						.setRegParam(regParam);
				LassoModel model = algorithm.run(trainingDataset.rdd());
				
				LOGGER.debug("Computing regression metrics for Lasso model; numIterations={}, stepSize={}, regularizationParameter={}", numIterations, stepSize, regParam);
				RegressionMetrics trainingRegressionMetrics = getRegressionMetricsForDataset(trainingDataset, model);
				RegressionMetrics testingRegressionMetrics = getRegressionMetricsForDataset(testingDataset, model);
								
				sb.append(String.format("%d, %5.3f, %5.3f, %.4f, %.4f\n", numIterations, stepSize, regParam, trainingRegressionMetrics.rootMeanSquaredError(), testingRegressionMetrics.rootMeanSquaredError()));				
			})));
		
		System.out.println("Printing statistics in CSV format:");
		System.out.println(sb.toString());
	}
	
	private static void iterateRidgeRegressionWithSGD(List<Integer> numIterationsList, List<Double> stepSizes, List<Double> regParams, JavaRDD<LabeledPoint> trainingDataset, JavaRDD<LabeledPoint> testingDataset) {
		StringBuffer sb = new StringBuffer();
		sb.append("numIters, stepSize, regParam, RMSE (training), RMSE(testing)\n");
		
		numIterationsList.stream()
			.forEach(numIterations -> stepSizes.stream().forEach(stepSize -> regParams.stream().forEach(regParam -> {
				LOGGER.debug("Creating Lasso model for numIterations={}, stepSize={}, regularizationParameter={}", numIterations, stepSize, regParam);
				RidgeRegressionWithSGD algorithm = new RidgeRegressionWithSGD();
				algorithm.setIntercept(true);
				algorithm
					.optimizer()
						.setNumIterations(numIterations)
						.setStepSize(stepSize)
						.setRegParam(regParam);
				RidgeRegressionModel model = algorithm.run(trainingDataset.rdd());
				
				LOGGER.debug("Computing regression metrics for Lasso model; numIterations={}, stepSize={}, regularizationParameter={}", numIterations, stepSize, regParam);
				RegressionMetrics trainingRegressionMetrics = getRegressionMetricsForDataset(trainingDataset, model);
				RegressionMetrics testingRegressionMetrics = getRegressionMetricsForDataset(testingDataset, model);
								
				sb.append(String.format("%d, %5.3f, %5.3f, %.4f, %.4f\n", numIterations, stepSize, regParam, trainingRegressionMetrics.rootMeanSquaredError(), testingRegressionMetrics.rootMeanSquaredError()));				
			})));
		
		System.out.println("Printing statistics in CSV format:");
		System.out.println(sb.toString());
	}
	
	
	
	private static Vector getVectorWithHigherOrderPolynomials(Vector v) {
		return Vectors.dense(Arrays.stream(v.toArray()).flatMap(vElem -> Arrays.stream(new double[] {vElem, vElem * vElem})).toArray());
	}
	
	private static void iterateLinearRegressionWithSGD(List<Integer> numIterationsList, List<Double> stepSizes, JavaRDD<LabeledPoint> trainingDataset, JavaRDD<LabeledPoint> testingDataset) {
		StringBuffer sb = new StringBuffer();
		sb.append("numIters, stepSize, RMSE (training), RMSE(testing)\n");

		numIterationsList.stream()
			.forEach(numIterations -> stepSizes.stream().forEach(stepSize -> {
				LOGGER.debug("Creating model for numIterations={}, stepSize={}", numIterations, stepSize);
				LinearRegressionWithSGD algorithm = new LinearRegressionWithSGD();
				algorithm.setIntercept(true);
				algorithm
					.optimizer()
						.setNumIterations(numIterations)
						.setStepSize(stepSize);
				LinearRegressionModel model = algorithm.run(trainingDataset.rdd());
				
				LOGGER.debug("Computing regression metrics for numIterations={}, stepSize={}", numIterations, stepSize);
				RegressionMetrics trainingRegressionMetrics = getRegressionMetricsForDataset(trainingDataset, model);
				RegressionMetrics testingRegressionMetrics = getRegressionMetricsForDataset(testingDataset, model);
								
				sb.append(String.format("%d, %5.3f, %.4f, %.4f\n", numIterations, stepSize, trainingRegressionMetrics.rootMeanSquaredError(), testingRegressionMetrics.rootMeanSquaredError()));				
			}));
		
		System.out.println("Printing statistics in CSV format:");
		System.out.println(sb.toString());
	}
	
	
	private static RegressionMetrics getRegressionMetricsForDataset(JavaRDD<LabeledPoint> dataset, GeneralizedLinearModel model) {
		JavaPairRDD<Object, Object> predictionsAndObserviations = dataset.mapToPair(labeledPoint -> new Tuple2<>(model.predict(labeledPoint.features()), labeledPoint.label()));
		return new RegressionMetrics(predictionsAndObserviations.rdd());
	}
	

	private static void prettyPrintDataset(JavaRDD<LabeledPoint> dataset) {
		dataset
			.collect()
			.stream()
			.forEach(labeledPoint -> {
				System.out.print(labeledPoint.label() + ": ");
				Arrays.stream(labeledPoint.features().toArray()).forEach(num -> System.out.print(num + ", "));
				System.out.println();
			});
	}
	
	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}	
}
