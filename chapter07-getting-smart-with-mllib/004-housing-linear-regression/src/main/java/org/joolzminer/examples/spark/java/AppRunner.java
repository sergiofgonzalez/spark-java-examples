package org.joolzminer.examples.spark.java;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		
		SparkConf config = new SparkConf()
				.setAppName("004-housing-linear-regression")
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
			 * __Phase 1: Familiarizing with the data set__
			 * 
			 * Step 1.a: calculate the `multivariate statistical summary`
			 */
			RowMatrix housingValuesMatrix = new RowMatrix(housingValues.rdd());
			MultivariateStatisticalSummary housingStatistics = housingValuesMatrix.computeColumnSummaryStatistics();
			System.out.println("Multivariate Statistical Summary:");
			System.out.println("Count   :\t" + housingStatistics.count());
			System.out.println("Min     :\t" + getPrettyVectorString(housingStatistics.min()));
			System.out.println("Max     :\t" + getPrettyVectorString(housingStatistics.max()));
			System.out.println("Variance:\t" + getPrettyVectorString(housingStatistics.variance()));
			System.out.println("Std dev :\t" + getPrettyVectorString(getStdDevVector(housingStatistics)));
			System.out.println("Mean    :\t" + getPrettyVectorString(housingStatistics.mean()));
			printSeparator();		
			
			/*
			 * Step 1.b: calculate the `multivariate summary statistics` using the Statistics object
			 */
			MultivariateStatisticalSummary housingDatasetStatistics = Statistics.colStats(housingValues.rdd());
			System.out.println("Multivariate Statistical Summary:");
			System.out.println("Count   :\t" + housingDatasetStatistics.count());
			System.out.println("Min     :\t" + getPrettyVectorString(housingDatasetStatistics.min()));
			System.out.println("Max     :\t" + getPrettyVectorString(housingDatasetStatistics.max()));
			System.out.println("Variance:\t" + getPrettyVectorString(housingDatasetStatistics.variance()));
			System.out.println("Std dev :\t" + getPrettyVectorString(getStdDevVector(housingDatasetStatistics)));
			System.out.println("Mean    :\t" + getPrettyVectorString(housingDatasetStatistics.mean()));
			printSeparator();			
			
			
			/*
			 * Step 2: Analyzing column cosine similarities
			 * 
			 * The value at (i, j) gives a measure of the similarity between column i and column j,
			 * thus, the values of the last column gives you the similarities between column 13 (price)
			 * and the rest of the columns.
			 * 
			 */
			CoordinateMatrix housingColumnSimilarities = housingValuesMatrix.columnSimilarities();
			System.out.println("Column cosine similarities:");
			prettyPrint(housingColumnSimilarities);
			printSeparator();
			
			/*
			 * Step 3: Analyzing the covariance matrix
			 * 
			 * + The matrix is symmetric.
			 * + Measures how column i is related to column, j:
			 *    - if 0, no linear relationship
			 *    - the higher the number, more linearly related
			 */
			Matrix housingCovarianceMatrix = housingValuesMatrix.computeCovariance();
			System.out.println("Covariance matrix:");
			prettyPrint(housingCovarianceMatrix);
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
			housingData.foreach(System.out::println);
			printSeparator();
			
			/*
			 * Step 2: Splitting the data into training and testing data sets
			 */
			JavaRDD<LabeledPoint>[] dataSets = housingData.randomSplit(new double[] {0.8, 0.2});
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
			
			JavaRDD<LabeledPoint> housingDataSetTestingScaled = housingDatasetTesting
					.map(labeledPt -> new LabeledPoint(labeledPt.label(),  fittedScaler.transform(labeledPt.features())));
			
			printDatasetInCsv(housingDataSetTrainingScaled, 5);
			printSeparator();
			
			
			
			
			/*
			 * __Phase 3: Creating the model__
			 * 
			 * Step 1.a: Creating the model using the train method 
			 * (which does not provide the intercept value)
			 * 
			 */
			LinearRegressionModel sparkWayModel = LinearRegressionWithSGD.train(housingDataSetTrainingScaled.rdd(), 200, 1.0);
			System.out.println("Model using train(): " + sparkWayModel);
			printSeparator();
			
			/*
			 * 
			 * Step 1.b: Creating the model using the custom method (RECOMMENDED) 
			 * 
			 * parameters for SGD:
			 * 	+ numIterations = 200
			 */
			LinearRegressionWithSGD algorithm = new LinearRegressionWithSGD();
			algorithm.setIntercept(true);
			algorithm.optimizer().setNumIterations(200);
			
			housingDataSetTrainingScaled.cache();
			housingDataSetTestingScaled.cache();
			
			LinearRegressionModel housingLinearRegressionModel = algorithm.run(housingDataSetTrainingScaled.rdd());
			
			System.out.println("housingLinearRegressionModel: " + housingLinearRegressionModel);
			System.out.println("weights: " + housingLinearRegressionModel.weights());
			printSeparator();
			
			/*
			 * Step 2: Evaluating the model: first steps
			 * 
			 */
			JavaPairRDD<Double, Double> testPredictionsScaled = housingDataSetTestingScaled
					.mapToPair(labeledPoint -> new Tuple2<>(housingLinearRegressionModel.predict(labeledPoint.features()), labeledPoint.label()));
			
			prettyPrintPredictions(testPredictionsScaled);
			printSeparator();
			
			// Calculating the root mean squared error (RMSE)
			Double localRMSE = Math.sqrt(testPredictionsScaled
				.collect()
				.stream()
				.mapToDouble(tuple2 -> Math.pow(tuple2._1() - tuple2._2, 2.0))
				.average()
				.getAsDouble());
			
			System.out.println("RMSE=" + localRMSE);
			
			// Calculating the root mean squared error without collecting (useful for very large datasets)
			double numElems = testPredictionsScaled.count();
			Double parallelRMSE = Math.sqrt((1 / numElems) * testPredictionsScaled
				.map(tuple2 -> Math.pow(tuple2._1() - tuple2._2, 2D))
				.reduce((acc, val) -> acc + val));
			System.out.println(String.format("RMSE=%f", parallelRMSE));
			printSeparator();	
			
			/*
			 * Step 3: Evaluating the model's performance
			 *
			 *  Using Spark's RegressionMetrics
			 *  
			 *  r2 is a number between 0 and 1 that measures how much a model accounts to the
			 *  variance of the target variable.
			 *  (the explained variance is a similar coefficient)
			 */
			
			// this is a drag!!!
			JavaPairRDD<Object, Object> testPredictionsForRegressionMetrics = 
					testPredictionsScaled.mapToPair(tuple2 -> new Tuple2<Object, Object>(tuple2._1(), tuple2._2()));
			
			RegressionMetrics testPredictionsScaledMetrics = new RegressionMetrics(testPredictionsForRegressionMetrics.rdd());
			System.out.println("Regression Metrics for test predictions:");
			System.out.println("root mean squared error: " + testPredictionsScaledMetrics.rootMeanSquaredError());
			System.out.println("mean squared error     : " + testPredictionsScaledMetrics.meanSquaredError());
			System.out.println("mean absolute error    : " + testPredictionsScaledMetrics.meanAbsoluteError());
			System.out.println("r2                     : " + testPredictionsScaledMetrics.r2());
			System.out.println("explained variance     : " + testPredictionsScaledMetrics.explainedVariance());
			printSeparator();
			
			
			/* 
			 * Step 4: Interpreting model parameters 
			 * 
			 * We obtain the weights, if a particular weight is ~0 means that the
			 * corresponding feature does not contribute much to the target variable.
			 * 
			 */
			double[] weights = housingLinearRegressionModel.weights().toArray();
			for (int i = 0; i < weights.length; i++) {
				System.out.println("Feature #" + i + ": " + weights[i] * weights[i]);
			}
			printSeparator();
			
			/*
			 * __Phase 4: Saving the model__
			 * 
			 *  We're done, let's save the model for the future
			 *  
			 */
			System.out.println("Persisting the model to file... (previous model will be deleted)");
			String modelPath = "housing-linear-regression-model";
			FileUtils.deleteDirectory(Paths.get(datasetPath, modelPath).toFile());
			
			housingLinearRegressionModel.save(sc.sc(), Paths.get(datasetPath, modelPath).toString());
			printSeparator();
		}														
	}
	
	private static Vector getStdDevVector(MultivariateStatisticalSummary mvsm) {
		return Vectors.dense(Arrays.stream(mvsm.variance()
									.toArray())
									.map(variance -> Math.sqrt(variance))
									.toArray());
	}


	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
	private static String getPrettyVectorString(Vector v) {
		return Arrays.stream(v.toArray()).mapToObj(String::valueOf).collect(joining(", "));	
	}
	
	private static void prettyPrint(CoordinateMatrix coordinateMatrix) {
		if (coordinateMatrix.numRows() > Integer.MAX_VALUE || coordinateMatrix.numCols() > Integer.MAX_VALUE) {
			LOGGER.error("Cannot create array: either numRows or numCols are larger than the max integer: rows={}; cols={}", coordinateMatrix.numRows(), coordinateMatrix.numCols());
			throw new IllegalStateException("Cannot handle such huge matrices locally");
		}
		final double[][] localMatrix = new double[(int)coordinateMatrix.numRows()][(int)coordinateMatrix.numCols()];
		coordinateMatrix.entries().toJavaRDD().collect().forEach(entry -> localMatrix[(int)entry.i()][(int)entry.j()] = entry.value());
	
		for (int i = 0; i < (int) coordinateMatrix.numRows(); i++) {
			for (int j = 0; j < (int) coordinateMatrix.numCols(); j++) {
				System.out.print(String.format("%.3f", localMatrix[i][j]));
				System.out.print(" ");
			}
			System.out.println();
		}
	}
	
	private static void prettyPrint(Matrix matrix) {
		
		int numRows = matrix.numRows();
		int numCols = matrix.numCols();
		
		double[] matrixAsArray = matrix.toArray();
			
		for (int i = 0; i < numRows; i++) {
			for (int j = 0; j < numCols; j++) {
				System.out.print(String.format("%9.3f", matrixAsArray[j * numCols + i]));
				System.out.print(" ");
			}
			System.out.println();
		}
	}
	
	private static void printDatasetInCsv(JavaRDD<LabeledPoint> dataSet, int colIndex) {
		dataSet.collect().stream()
			.forEach(labeledPoint -> System.out.println(labeledPoint.features().toArray()[colIndex] + ", " + labeledPoint.label()));
	}
	
	private static void prettyPrintPredictions(JavaPairRDD<Double, Double> predictions) {
		System.out.println("   Prediction\t||   Testing Value");
		predictions.collect().stream()
			.forEach(tuple2 -> System.out.println(String.format("%12.5f \t|| %12.5f", tuple2._1(), tuple2._2())));
	}
}
