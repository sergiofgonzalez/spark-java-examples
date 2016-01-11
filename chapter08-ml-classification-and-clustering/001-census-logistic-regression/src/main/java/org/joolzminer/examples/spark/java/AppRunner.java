package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;


public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
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
			censusDataFrame.show();
			
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
			
			/*
			censusDataFrame.groupBy("occupation").count().foreach(new DataFrameRowPrinter());
			censusDataFrame.groupBy("native_country").count().foreach(new DataFrameRowPrinter());
			*/
		}														
	}

	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
}
