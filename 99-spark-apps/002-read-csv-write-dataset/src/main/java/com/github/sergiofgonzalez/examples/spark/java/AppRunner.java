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
						.afterSparkInitializedDo(compressionHook)
						.submit());		
	}
	
	

	

//	SqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed");
	private static final BiConsumer<SparkSession, JavaSparkContext> compressionHook = (spark, sparkContext) -> {
		spark.sqlContext().setConf("spark.sql.parquet.compression.codec", wconf().get("files.compression")); // snappy, gzip, uncompressed
	};	
	
	/* The Spark application to execute */
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
