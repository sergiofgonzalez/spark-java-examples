package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
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
				.setAppName("005-handwritten-digits-kmeans-clustering")
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
			
			JavaPairRDD<Vector, Double> labeledPointsRDD = labeledPointsDataFrame.toJavaRDD().mapToPair(row -> new Tuple2<>((Vector)row.get(0), (Double)row.get(1)));
			JavaRDD<Vector> penDatasetJavaRDD = labeledPointsRDD.map(tuple2 -> (Vector)tuple2._1());
			RDD<Vector> penDatasetRDD = JavaRDD.toRDD(penDatasetJavaRDD);
			penDatasetRDD.cache();
			System.out.println(penDatasetRDD.first());
			
			KMeansModel kMeansClusteringModel = KMeans.train(penDatasetRDD, 10, 5000, 20);
			System.out.println("Cost:" + kMeansClusteringModel.computeCost(penDatasetRDD));
			System.out.println("Avg distance from center:" + Math.sqrt(kMeansClusteringModel.computeCost(penDatasetRDD)/penDatasetRDD.count()));
			printSeparator();
			
			JavaPairRDD<Double, Double> kMeansPredictions = labeledPointsRDD.mapToPair(tuple2 -> new Tuple2<>((double)kMeansClusteringModel.predict(tuple2._1()), tuple2._2));
			System.out.println(kMeansPredictions.first());
		}														
	}
	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
}
