package com.github.sergiofgonzalez.examples.spark.java;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;
import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {		
		withSparkDo(cfg -> cfg
						.withName("ReadCsvWriteDatasetApp")
						.withPropertiesFile("config/spark-job.conf")
						.withExtraConfigVars(args)
						.doing(csvTestApp)
						.afterSparkInitializedDo(initializationHook)
						.submit());		
	}
	
	
	
	

	private static final BiConsumer<SparkSession, JavaSparkContext> compressionHook = (spark, sparkContext) -> {
		String compressionMethod = wconf().get("files.compression");
		LOGGER.debug("Setting compression method to {}", compressionMethod);
		spark.sqlContext().setConf("spark.sql.parquet.compression.codec", wconf().get("files.compression")); // snappy, gzip, uncompressed	
	}; 
		

	private static final BiConsumer<SparkSession, JavaSparkContext> cloudStorageHook = (spark, sparkContext) -> {
		String storageType = wconf().get("storage_type");
		
		switch (storageType) {
			case "aws":
				LOGGER.debug("Running the tests on AWS S3");
				sparkContext.hadoopConfiguration().set("fs.s3a.access.key", wconf().get("access_key_id"));
				sparkContext.hadoopConfiguration().set("fs.s3a.secret.key", wconf().get("secret_access_key"));
				break;
			
			case "azure":
				LOGGER.debug("Running the tests on Azure Blob Storage");	
				sparkContext.hadoopConfiguration().set(String.format("fs.azure.account.key.%s.blob.core.windows.net", wconf().get("storage_account")), wconf().get("storage_key"));					
				break;
			

			case "local":				
				LOGGER.debug("Running the tests on the local file system");
				break;
				
			default:
				LOGGER.error("The storage type {} is not an expcted value. Expected values: local|aws|azure", storageType);
				throw new IllegalArgumentException("Unrecognized value for storageType");
		}
	};	
	
	private static final BiConsumer<SparkSession, JavaSparkContext> initializationHook = (spark, sparkContext) -> {
		compressionHook.andThen(cloudStorageHook).accept(spark, sparkContext);
	}; 
	
	
	
	
	@SuppressWarnings("serial")
	private static final BiConsumer<SparkSession, JavaSparkContext> sensorCsvTestApp = (spark, sparkContext) -> {

		List<StructField> schemaFields = new ArrayList<StructField>() {{
			add(DataTypes.createStructField("c1", DataTypes.StringType, false));
			add(DataTypes.createStructField("c2", DataTypes.StringType, false));
			add(DataTypes.createStructField("c3", DataTypes.StringType, false));
			add(DataTypes.createStructField("c4", DataTypes.TimestampType, false));	
			add(DataTypes.createStructField("c5", DataTypes.DoubleType, false));	
		}};
		
		StructType sensorSchema = DataTypes.createStructType(schemaFields);
		
		String inputFile = wconf().get("files.input");
		LOGGER.debug("About to load in dataset {}", inputFile);
		
		Dataset<Row> dataset = spark								
								.read()
								.schema(sensorSchema)
								.option("header", false)
								.option("mode", "FAILFAST")
								.option("timestampFormat", wconf().get("files.timestamp_format"))
								.csv(wconf().get("files.input"));
								
		
		dataset.printSchema();
		System.out.println("Input CSV record count: " + dataset.count());

		dataset.write()
			.mode(SaveMode.Overwrite)
			.parquet(wconf().get("files.output_path"));
	
		Dataset<Row> savedParquet = spark.read().parquet(wconf().get("files.output_path"));
		savedParquet.show(10, false);
		System.out.println("SavedParquet record count: " + savedParquet.count());

	};

	
	@SuppressWarnings("serial")
	private static final BiConsumer<SparkSession, JavaSparkContext> spillCsvTestApp = (spark, sparkContext) -> {

		List<StructField> schemaFields = new ArrayList<StructField>() {{
			add(DataTypes.createStructField("c1", DataTypes.TimestampType, false));

			add(DataTypes.createStructField("c2", DataTypes.IntegerType, false));
			add(DataTypes.createStructField("c3", DataTypes.StringType, false));
			add(DataTypes.createStructField("c4", DataTypes.StringType, false));
			add(DataTypes.createStructField("c5", DataTypes.StringType, false));
			add(DataTypes.createStructField("c6", DataTypes.IntegerType, false));

			add(DataTypes.createStructField("c7", DataTypes.TimestampType, false));
			add(DataTypes.createStructField("c8", DataTypes.StringType, false));
			add(DataTypes.createStructField("c9", DataTypes.StringType, false));
			add(DataTypes.createStructField("c10", DataTypes.StringType, false));

			add(DataTypes.createStructField("c11", DataTypes.TimestampType, false));
			add(DataTypes.createStructField("c12", DataTypes.TimestampType, false));
			add(DataTypes.createStructField("c13", DataTypes.TimestampType, false));	

			add(DataTypes.createStructField("c14", DataTypes.StringType, false));	
			add(DataTypes.createStructField("c15", DataTypes.IntegerType, false));
			add(DataTypes.createStructField("c16", DataTypes.IntegerType, false));		

			add(DataTypes.createStructField("c17", DataTypes.TimestampType, false));
			add(DataTypes.createStructField("c18", DataTypes.StringType, false));
			add(DataTypes.createStructField("c19", DataTypes.StringType, false));	

			add(DataTypes.createStructField("c20", DataTypes.IntegerType, false));		
			add(DataTypes.createStructField("c21", DataTypes.IntegerType, false));	
			add(DataTypes.createStructField("c22", DataTypes.IntegerType, false));		

			add(DataTypes.createStructField("c23", DataTypes.DoubleType, false));		
			add(DataTypes.createStructField("c24", DataTypes.DoubleType, false));	

			add(DataTypes.createStructField("c25", DataTypes.StringType, false));
			
			add(DataTypes.createStructField("c26", DataTypes.DoubleType, false));		
			add(DataTypes.createStructField("c27", DataTypes.DoubleType, false));
			add(DataTypes.createStructField("c28", DataTypes.StringType, false));
			add(DataTypes.createStructField("c29", DataTypes.IntegerType, false));	
			add(DataTypes.createStructField("c30", DataTypes.IntegerType, false));					
		}};
		
		StructType sensorSchema = DataTypes.createStructType(schemaFields);
		
		Dataset<Row> dataset = spark								
								.read()
								.schema(sensorSchema)
								.option("header", false)
								.option("mode", "FAILFAST")
								.option("timestampFormat", wconf().get("files.timestamp_format"))
								.csv(wconf().get("files.input"));
								
		
		dataset.printSchema();
		System.out.println("Input CSV record count: " + dataset.count());

		dataset.write()
			.mode(SaveMode.Overwrite)
			.parquet(wconf().get("files.output_path"));
		
		Dataset<Row> savedParquet = spark.read().parquet(wconf().get("files.output_path"));
		savedParquet.show(10, false);
		System.out.println("SavedParquet record count: " + savedParquet.count());

	};	
		
	
	private static final BiConsumer<SparkSession, JavaSparkContext> csvTestApp = (sparkSession, sparkContext) -> {
		if (wconf().get("mode").equalsIgnoreCase("sensor")) {
			LOGGER.debug("Running test on Sensor CSV data");
			sensorCsvTestApp.accept(sparkSession, sparkContext);
		} else if (wconf().get("mode").equalsIgnoreCase("spill")) {
			LOGGER.debug("Running test on Spill CSV data");
			spillCsvTestApp.accept(sparkSession, sparkContext);
		} else {
			LOGGER.error("Unsupported mode {}. Must use either spill or sensor");
			throw new UnsupportedOperationException("Unsupported mode");
		}
	};

}
