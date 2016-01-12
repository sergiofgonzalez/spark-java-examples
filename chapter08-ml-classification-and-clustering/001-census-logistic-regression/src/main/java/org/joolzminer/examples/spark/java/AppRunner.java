package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		
		SparkConf config = new SparkConf()
				.setAppName("001-census-logistic-regression")
				.setMaster("local[*]");
		
		try (JavaSparkContext sc = new JavaSparkContext(config)) {
						
			SQLContext sqlContext = new SQLContext(sc);
			
			/*
			 * __Phase 0: Acquiring data__
			 * 
			 * Step 0: load the dataset lines
			 */
			String datasetPath = "./src/main/resources/census-dataset";
			String datasetFilename = "adult-mod.csv";
			
			JavaRDD<String> censusDatasetLines = sc.textFile(Paths.get(datasetPath, datasetFilename).toString(), 6);
			long numLines = censusDatasetLines.count();
			System.out.println("\nLoaded " + numLines + " line(s) from " + datasetFilename);
			printSeparator();
			
			
			/*
			 * Step 1: Convert types to the appropriate type
			 */
						
			JavaRDD<Row> censusRows = censusDatasetLines.map(line -> {
				String[] strFields = line.split(", ");
				return RowFactory.create(Double.parseDouble(strFields[0]),
											strFields[1],
											Double.parseDouble(strFields[2]),
											strFields[3],
											strFields[4],
											strFields[5],
											strFields[6],
											strFields[7],
											strFields[8],
											Double.parseDouble(strFields[9]),
											Double.parseDouble(strFields[10]),
											Double.parseDouble(strFields[11]),
											strFields[12],
											strFields[13]);
			});
			
			
			/*
			 * Step 2: Create the schema of the data in the RDD
			 */
			List<StructField> schemaFields = new ArrayList<>();
			schemaFields.add(DataTypes.createStructField("age", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("workclass", DataTypes.StringType, true));
			schemaFields.add(DataTypes.createStructField("fnlwgt", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("education", DataTypes.StringType, true));
			schemaFields.add(DataTypes.createStructField("marital_status", DataTypes.StringType, true));
			schemaFields.add(DataTypes.createStructField("occupation", DataTypes.StringType, true));	
			schemaFields.add(DataTypes.createStructField("relationship", DataTypes.StringType, true));
			schemaFields.add(DataTypes.createStructField("race", DataTypes.StringType, true));	
			schemaFields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
			schemaFields.add(DataTypes.createStructField("capital_gain", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("capital_loss", DataTypes.DoubleType, true));		
			schemaFields.add(DataTypes.createStructField("hours_per_week", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("native_country", DataTypes.StringType, true));	
			schemaFields.add(DataTypes.createStructField("income", DataTypes.StringType, true));
			
			StructType schema = DataTypes.createStructType(schemaFields);
			
			
			/*
			 * Step 3: Create the DataFrame that represents the data
			 */
			DataFrame censusDataFrame = sqlContext.createDataFrame(censusRows, schema);
			censusDataFrame.show(10);
			
			/*
			 * Step 4: Search for missing values that the dataset may have and fix them using the simplest of strategies:
			 * 	+ apply the most common one 
			 * 
			 * Note: missing values in the dataset are identified as `?`
			 */

			// workclass column
			Row[] workclassCounts = censusDataFrame.groupBy("workclass").count().orderBy("count").collect();
			System.out.println("\nWorkclass values:");
			Arrays.stream(workclassCounts).forEach(System.out::println);
			
			// occupation column
			Row[] occupationCounts = censusDataFrame.groupBy("occupation").count().orderBy("count").collect();
			System.out.println("\nOccupation values:");
			Arrays.stream(occupationCounts).forEach(System.out::println);
			
			// native_country column
			Row[] nativeCountryCounts = censusDataFrame.groupBy("native_country").count().orderBy("count").collect();
			System.out.println("\nNative Country values:");
			Arrays.stream(nativeCountryCounts).forEach(System.out::println);
			
			
			DataFrame noNaCensusDataFrame = censusDataFrame.na().replace(new String[] {"workclass", "occupation"}, new HashMap<String, String>() {{
				put("?", "Private");
			}}).na().replace("native_country", new HashMap<String, String>() {{
				put("?", "United-States");
			}});
			
			noNaCensusDataFrame.show(10);
			
			/*
			 * Step 5: Transform string data to numeric values
			 * 
			 * + using one-hot encoding: a column is expanded to as many cols as there are distinct values so that
			 *   only one column contains a 1 and all the others contain zeros.
			 *   
			 *   Note: The resulting columns are vectors.
			 */
			
			// 1: convert string columns to numeric values
			DataFrame censusNumericDataFrame = indexStringColumns(noNaCensusDataFrame, Arrays.asList("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"));
			censusNumericDataFrame.show(10);
			
			// 2: Apply one-hot encoding strategy
			DataFrame censusHotEncodedDataFrame = oneHotEncodeColumns(censusNumericDataFrame, Arrays.asList("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country"));
			censusHotEncodedDataFrame.show(10);
			
			/*
			 * Step 6: Merge new vectors and original numeric columns into a single vector column containing all the features.
			 */
			VectorAssembler vectorAssembler = new VectorAssembler().setOutputCol("features");
			vectorAssembler.setInputCols(new String[] {"age", "workclass", "fnlwgt", "education", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country"});
			DataFrame labeledPointsDataFrame = vectorAssembler.transform(censusHotEncodedDataFrame).select("features", "income").withColumnRenamed("income", "label");
			labeledPointsDataFrame.show(10);
			
			/*
			 * __Phase 1: Fitting the Model__
			 * 
			 * Step 1: Split the data into training and testing data
			 */
			
			DataFrame[] splits = labeledPointsDataFrame.randomSplit(new double[]{0.8, 0.2});
			DataFrame censusTrainingDataFrame = splits[0].cache();
			DataFrame censusTestingDataFrame = splits[1].cache();

			/*
			 * Step 2: Create a Logistic Regression Model
			 *  + Create a LogisticRegression object and obtain the model by fitting the training dataset.
			 */
			
			// Create a LogisticRegression object and obtain the model by fitting the training dataset. 
			LogisticRegression logisticRegression = new LogisticRegression();
			logisticRegression
				.setRegParam(0.01)
				.setMaxIter(1000)
				.setFitIntercept(true);
						
			 LogisticRegressionModel lrModel = logisticRegression.fit(censusTrainingDataFrame);
			 System.out.println("weights  : " + lrModel.weights());
			 System.out.println("intercept: " + lrModel.intercept());
			 printSeparator();
								
			/*
			 * __Phase 2: Evaluates the model performance__
			 * 
			 * Step 1: obtain the testing data frame by applying the model to the testing data frame
			 */
			 
			 DataFrame testPredictionsDataFrame = lrModel.transform(censusTestingDataFrame);
			 testPredictionsDataFrame.show();

			 /*
			  * Step 2: Use a BinaryClassificationEvaluator to obtain evaluation metrics
			  * + By default we get the area under receiver operating characteristic curve (areaUnderROC),
			  * but we can also configure it to obtain the area under precision recall curve (areaUnderPR).
			  * 
			  * Both ROC and PR curves are used for comparing different models.
			  */
			 BinaryClassificationEvaluator binaryClassificationEvaluator = new BinaryClassificationEvaluator();
			 double metricValue = binaryClassificationEvaluator.evaluate(testPredictionsDataFrame);			 
			 
			 System.out.println(binaryClassificationEvaluator.getMetricName() + " = " + metricValue);
			 printSeparator();
			 
			 binaryClassificationEvaluator.setMetricName("areaUnderPR");
			 metricValue = binaryClassificationEvaluator.evaluate(testPredictionsDataFrame);
			 System.out.println(binaryClassificationEvaluator.getMetricName() + " = " + metricValue);
			 printSeparator();			 
			 
			 // Obtain the Precision-Recall (PR) curve
			 // Updating the threshold of the your model for the Binary classification
			 // (The areaUnderPR gives us the area under this curve)
			 computePRCurve(censusTestingDataFrame, lrModel);
			 printSeparator();
			 
			 // Obtain the Receiver Operating Characteristic (ROC) curve
			 computeROCCurve(censusTestingDataFrame, lrModel);
			 printSeparator();
			 
			/*
			 * __Phase 3: Performing k-fold cross validation__
			 * 
			 * In short, k-fold cross validation consists of dividing the data set into k subsets of equal
			 * sizes and training k models excluding a different subset each time (which will be used as the 
			 * testing data set and all the other sets together as the training data set).
			 * Then, you train all k models and choose the one with the smallest average error.
			 */
			 System.out.println("Performing k-fold cross validation: this may take a moment or two...");
			 CrossValidator crossValidator = new CrossValidator()
									 				.setEstimator(logisticRegression)
									 				.setEvaluator(binaryClassificationEvaluator)
									 				.setNumFolds(5);
			 
			 ParamMap[] paramGrid = new ParamGridBuilder()
						 		.addGrid(logisticRegression.maxIter(), new int[] {1000})
						 		.addGrid(logisticRegression.regParam(), new double[] {0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5})
						 		.build();
			 
			 crossValidator.setEstimatorParamMaps(paramGrid);
			 
			 CrossValidatorModel crossValidatorModel = crossValidator.fit(censusTrainingDataFrame);
			 
			 // Obtain the weights of the best logistic regression model among the k models
			 LogisticRegressionModel bestLogisticRegressionModel = (LogisticRegressionModel) crossValidatorModel.bestModel();
			 System.out.println("weights  : " + bestLogisticRegressionModel.weights());
			 System.out.println("intercept: " + bestLogisticRegressionModel.intercept());
			 printSeparator();
			 
			 // Obtain the selected regularization parameter
			 LogisticRegression bestLogisticRegression = (LogisticRegression) bestLogisticRegressionModel.parent();
			 System.out.println("Selected Regularization Parameter: " + bestLogisticRegression.getRegParam());
			 printSeparator();
			 
			 // Evaluate the performance of the best model
			 binaryClassificationEvaluator = new BinaryClassificationEvaluator();
			 metricValue = binaryClassificationEvaluator.evaluate(bestLogisticRegressionModel.transform(censusTestingDataFrame));			 			 
			 System.out.println(binaryClassificationEvaluator.getMetricName() + " = " + metricValue);
			 printSeparator();
			 
			 binaryClassificationEvaluator.setMetricName("areaUnderPR");
			 metricValue = binaryClassificationEvaluator.evaluate(bestLogisticRegressionModel.transform(censusTestingDataFrame));
			 System.out.println(binaryClassificationEvaluator.getMetricName() + " = " + metricValue);
			 printSeparator();			 			 
			 
		}														
	}

	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
	private static DataFrame indexStringColumns(DataFrame df, List<String> cols) {
		DataFrame newDataFrame = df;
		for (String col : cols) {
			StringIndexer stringIndexer = new StringIndexer().setInputCol(col).setOutputCol(col + "-num");
			StringIndexerModel stringIndexerModel = stringIndexer.fit(newDataFrame);
			newDataFrame = stringIndexerModel.transform(newDataFrame).drop(col);
			newDataFrame = newDataFrame.withColumnRenamed(col + "-num", col);
		}
		return newDataFrame;
	}
	
	private static DataFrame oneHotEncodeColumns(DataFrame df, List<String> cols) {
		DataFrame newDataFrame = df;
		for (String col : cols) {
			OneHotEncoder oneHotEncoder = new OneHotEncoder().setInputCol(col);
			oneHotEncoder.setOutputCol(col + "-onehot").setDropLast(false);
			newDataFrame = oneHotEncoder.transform(newDataFrame).drop(col);
			newDataFrame = newDataFrame.withColumnRenamed(col + "-onehot", col);			
		}
		return newDataFrame;
	}
	
	@SuppressWarnings("unchecked")
	private static void computePRCurve(DataFrame testingDf, LogisticRegressionModel lrModel) {
		for (int i = 0; i <= 10; i++) {
			double threshold = i / 10.0;
			if (i == 10) {
				threshold -= 0.001;
			}
			lrModel.setThreshold(threshold);
			DataFrame testPredictionsDataFrame = lrModel.transform(testingDf);
			JavaPairRDD<Object,Object> testPredictionsRdd = testPredictionsDataFrame.toJavaRDD().mapToPair(row -> new Tuple2<>(row.getDouble(4), row.getDouble(1)));
			BinaryClassificationMetrics binaryClassificationMetrics = new BinaryClassificationMetrics(JavaPairRDD.toRDD(testPredictionsRdd));
			Tuple2<Double,Double>[] precisionRecallTuples = (Tuple2<Double, Double>[]) binaryClassificationMetrics.pr().collect();
			System.out.println(String.format("%.1f: R=%f, P=%f", threshold, precisionRecallTuples[1]._1(), precisionRecallTuples[1]._2()));
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void computeROCCurve(DataFrame testingDf, LogisticRegressionModel lrModel) {
		for (int i = 0; i <= 10; i++) {
			double threshold = i / 10.0;
			if (i == 10) {
				threshold -= 0.001;
			}
			lrModel.setThreshold(threshold);
			DataFrame testPredictionsDataFrame = lrModel.transform(testingDf);
			JavaPairRDD<Object, Object> testPredictionsRdd = testPredictionsDataFrame.toJavaRDD().mapToPair(row -> new Tuple2<>(row.getDouble(4), row.getDouble(1)));
			BinaryClassificationMetrics binaryClassificationMetrics = new BinaryClassificationMetrics(JavaPairRDD.toRDD(testPredictionsRdd));
			Tuple2<Double,Double>[] precisionRecallTuples = (Tuple2<Double, Double>[]) binaryClassificationMetrics.roc().collect();
			System.out.println(String.format("%.1f: FPR=%f, TPR=%f", threshold, precisionRecallTuples[1]._1(), precisionRecallTuples[1]._2()));			
		}
	}
}
