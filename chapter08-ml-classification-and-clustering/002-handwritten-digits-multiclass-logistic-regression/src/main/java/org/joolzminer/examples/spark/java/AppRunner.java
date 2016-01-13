package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
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
import java.util.List;


public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		
		SparkConf config = new SparkConf()
				.setAppName("002-handwritten-digits-multiclass-logistic-regression")
				.setMaster("local[*]");
		
		try (JavaSparkContext sc = new JavaSparkContext(config)) {
						
			SQLContext sqlContext = new SQLContext(sc);
			
			/*
			 * __Phase 0: Acquiring data__
			 * 
			 * Step 0: load the dataset lines
			 */
			String datasetPath = "./src/main/resources/handwritten-digits-dataset";
			String datasetFilename = "penbased.csv";
			
			JavaRDD<String> penDatasetLines = sc.textFile(Paths.get(datasetPath, datasetFilename).toString(), 4);
			long numLines = penDatasetLines.count();
			System.out.println("\nLoaded " + numLines + " line(s) from " + datasetFilename);
			printSeparator();
			
			
			/*
			 * Step 1: Convert types to the appropriate type
			 */
						
			JavaRDD<Row> penRows = penDatasetLines.map(line -> {
				String[] strFields = line.split(", ");
				return RowFactory.create(
						Double.parseDouble(strFields[0]),
						Double.parseDouble(strFields[1]),
						Double.parseDouble(strFields[2]),
						Double.parseDouble(strFields[3]),
						Double.parseDouble(strFields[4]),
						Double.parseDouble(strFields[5]),
						Double.parseDouble(strFields[6]),
						Double.parseDouble(strFields[7]),
						Double.parseDouble(strFields[8]),
						Double.parseDouble(strFields[9]),
						Double.parseDouble(strFields[10]),
						Double.parseDouble(strFields[11]),
						Double.parseDouble(strFields[12]),
						Double.parseDouble(strFields[13]),
						Double.parseDouble(strFields[14]),
						Double.parseDouble(strFields[15]),
						Double.parseDouble(strFields[16]));
				});
			
			
			/*
			 * Step 2: Create the schema of the data in the RDD
			 */
			List<StructField> schemaFields = new ArrayList<>();
			schemaFields.add(DataTypes.createStructField("pix1", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix2", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix3", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix4", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix5", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix6", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix7", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix8", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix9", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix10", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix11", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix12", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix13", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix14", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix15", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("pix16", DataTypes.DoubleType, true));
			schemaFields.add(DataTypes.createStructField("label", DataTypes.DoubleType, true));
			StructType schema = DataTypes.createStructType(schemaFields);
			
			
			/*
			 * Step 3: Create the DataFrame that represents the data
			 */
			DataFrame penDataFrame = sqlContext.createDataFrame(penRows, schema);
			penDataFrame.show(10);
			
			
			/*
			 * Step 6: Merge columns into a single vector column containing all the features.
			 */
			VectorAssembler vectorAssembler = new VectorAssembler().setOutputCol("features");
			vectorAssembler.setInputCols(new String[] {"pix1", "pix2", "pix3", "pix4", "pix5", "pix6", "pix7", "pix8", "pix9", "pix10", "pix11", "pix12", "pix13", "pix14", "pix15", "pix16",});
			DataFrame labeledPointsDataFrame = vectorAssembler.transform(penDataFrame).select("features", "label");
			labeledPointsDataFrame.show(10);
			
			/*
			 * __Phase 1: Fitting the Model__
			 * 
			 * Step 1: Split the data into training and testing data
			 */
			
			DataFrame[] splits = labeledPointsDataFrame.randomSplit(new double[]{0.8, 0.2});
			DataFrame penTrainingDataFrame = splits[0].cache();
			DataFrame penTestingDataFrame = splits[1].cache();

			/*
			 * Step 2: Create a Logistic Regression Model
			 *  + Create a LogisticRegression object and obtain the model by fitting the training dataset.
			 */
			
			// Create a LogisticRegression object and obtain the model by fitting the training dataset. 
			LogisticRegression logisticRegression = new LogisticRegression();
			logisticRegression.setRegParam(0.01);
						
			 
			// Using the one vs. rest strategy
			OneVsRest oneVsRest = new OneVsRest();
			oneVsRest.setClassifier(logisticRegression);
			
			OneVsRestModel oneVsRestModel = oneVsRest.fit(penTrainingDataFrame);
			
								
			/*
			 * __Phase 2: Evaluates the model performance__
			 * 
			 * Step 1: obtain the testing data frame by applying the model to the testing data frame
			 */
			 
			 DataFrame penTestPredictionsDataFrame = oneVsRestModel.transform(penTestingDataFrame);
			 penTestPredictionsDataFrame.show();

			 
			 /*
			  * Step 2: Convert to RDD to use MLlib's MulticlassMetrics
			  */
			 JavaPairRDD<Object, Object> penTestPredictionsJavaPairRDD = penTestPredictionsDataFrame.select("prediction", "label").toJavaRDD().mapToPair(row -> new Tuple2<>(row.getDouble(0), row.getDouble(1)));
			 MulticlassMetrics penMulticlassMetrics = new MulticlassMetrics(JavaPairRDD.toRDD(penTestPredictionsJavaPairRDD));
			 
			 /*
			  * Step 3: obtain the metrics
			  */
			 
			 // Note: Precision and recall are equal for multiclass classifiers because the sum of
			 // all positives is equal to the sum of all negatives
			 System.out.println("Precision: " + penMulticlassMetrics.precision());
			 System.out.println("Recall   : " + penMulticlassMetrics.recall());
			 printSeparator();
			 
			 // Obtaining the metrics per class
			 int numClasses = penMulticlassMetrics.labels().length; // 10 (digits from 0 to 9)
			 for (int i = 0; i < numClasses; i++) {
				 System.out.println("Precision and Recall for digit " + i);
				 System.out.println("\tPrecision: " + penMulticlassMetrics.precision(i));
				 System.out.println("\tRecall   : " + penMulticlassMetrics.precision(i));
				 printSeparator();				 
			 }
			 
			 // Obtaining the confusion matrix
			 // The element in (i, j) gives you the number of i class elements that were classified as class j
			 // e.g. (2, 3) gives you the number of 2's that were classified as 3's
			 System.out.println("\nConfusion Matrix:");
			 System.out.println(penMulticlassMetrics.confusionMatrix());
			 
		}														
	}

	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
}
