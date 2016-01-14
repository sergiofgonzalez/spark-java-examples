package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
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
			

			// This from official Spark documentation
			/*
			 * Index labels, adding metadata to the label column
			 * Fit on whole dataset to include all labels in index
			 */
			StringIndexerModel labelIndexer = new StringIndexer()
												.setInputCol("label")
												.setOutputCol("indexedLabel")
												.fit(labeledPointsDataFrame);
			
			/*
			 * Automatically identify categorical features and index them
			 */
			VectorIndexerModel featureIndexer = new VectorIndexer()
														.setInputCol("features")
														.setOutputCol("indexedFeatures")
														.setMaxCategories(10)	// features with > 10 distinct values will be treated as continuous
														.fit(labeledPointsDataFrame);
			
			
			/*
			 * __Phase 1: Fitting the Model__
			 * 
			 * Step 1: Split the data into training and testing data
			 */
			
			// Note that we're using the original data frame for the splits
			DataFrame[] splits = labeledPointsDataFrame.randomSplit(new double[]{0.8, 0.2});
			DataFrame penTrainingDataFrame = splits[0].cache();
			DataFrame penTestingDataFrame = splits[1].cache();
			
			
			// Train a Random Forest Model
			RandomForestClassifier randomForestClassifier = new RandomForestClassifier();
			randomForestClassifier.setMaxDepth(20);
			randomForestClassifier.setLabelCol("indexedLabel");
			randomForestClassifier.setFeaturesCol("indexedFeatures");
			
			// Convert indexed labels back to original labels
			IndexToString labelConverter = new IndexToString()
												.setInputCol("prediction")
												.setOutputCol("predictedLabel")
												.setLabels(labelIndexer.labels());
			
			// Chain indexers and tree in a Pipeline
			Pipeline pipeline = new Pipeline()
										.setStages(new PipelineStage[] {labelIndexer, featureIndexer, randomForestClassifier, labelConverter});
			
			// Train the model
			PipelineModel pipelineModel = pipeline.fit(penTrainingDataFrame);
			
			// Cast to a RandomForestClassificationModel to warp back to the book
			RandomForestClassificationModel randomForestClassificationModel = (RandomForestClassificationModel)(pipelineModel.stages()[2]);

			// In the book this is a categorical node, in my example it is continuous
			System.out.println(randomForestClassificationModel.trees());
			printSeparator();
			
			// The whole decision tree
			System.out.println(randomForestClassificationModel.toDebugString());
			printSeparator();
			
			// Construct the predictions
			DataFrame predictionsDataFrame = pipelineModel.transform(penTestingDataFrame);
			predictionsDataFrame.select("predictedLabel", "label", "features").show(10);
			
			
			// Evaluate
			MulticlassClassificationEvaluator multiclassClassificationEvaluator = new MulticlassClassificationEvaluator()
																						.setLabelCol("indexedLabel")
																						.setPredictionCol("prediction")
																						.setMetricName("precision");
			
			double precision = multiclassClassificationEvaluator.evaluate(predictionsDataFrame);
			System.out.println("Precision: " + precision);
			
			
			// Obtain the confusion matrix
			JavaPairRDD<Object, Object> penTestPredictionsJavaPairRDD = predictionsDataFrame.select("prediction", "indexedLabel").toJavaRDD().mapToPair(row -> new Tuple2<>(row.getDouble(0), row.getDouble(1)));
			MulticlassMetrics multiclassMetrics = new MulticlassMetrics(JavaPairRDD.toRDD(penTestPredictionsJavaPairRDD));
			
			System.out.println(multiclassMetrics.confusionMatrix());
			
		}														
	}

	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
}
