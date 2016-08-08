package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeInteger;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeLong;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeTimestamp;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import static org.apache.spark.sql.functions.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("DataFrames back to RDDs")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			SQLContext sqlContext = new SQLContext(sc);
			
			/*
			 * 0. Load a DataFrame from the input data set, using the explicit schema method
			 */
			Path inputDataPath = Paths.get("./src/main/resources", "italianPosts.csv");
			JavaRDD<String[]> inputDataFields = sc.textFile(inputDataPath.toString())
													.map(line -> line.split("~"));
			

			List<StructField> schemaFields = new ArrayList<StructField>() {{
				add(DataTypes.createStructField("commentCount", DataTypes.IntegerType, true));
				add(DataTypes.createStructField("lastActivityDate", DataTypes.TimestampType, true));
				add(DataTypes.createStructField("ownerUserId", DataTypes.StringType, true));
				add(DataTypes.createStructField("body", DataTypes.StringType, true));
				add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
				add(DataTypes.createStructField("creationDate", DataTypes.TimestampType, true));
				add(DataTypes.createStructField("viewCount", DataTypes.IntegerType, true));
				add(DataTypes.createStructField("title", DataTypes.StringType, true));
				add(DataTypes.createStructField("tags", DataTypes.StringType, true));
				add(DataTypes.createStructField("answerCount", DataTypes.IntegerType, true));
				add(DataTypes.createStructField("acceptedAnswerId", DataTypes.LongType, true));
				add(DataTypes.createStructField("postTypeId", DataTypes.LongType, true));
				add(DataTypes.createStructField("id", DataTypes.LongType, true));
			}};
			
			StructType schema = DataTypes.createStructType(schemaFields);
			
			JavaRDD<Row> rowRdd = inputDataFields.map(rowFields -> RowFactory.create(
					toSafeInteger(rowFields[0]),
					toSafeTimestamp(rowFields[1]),
					rowFields[2],
					rowFields[3],
					toSafeInteger(rowFields[4]),
					toSafeTimestamp(rowFields[5]),
					toSafeInteger(rowFields[6]),
					rowFields[7],
					rowFields[8],
					toSafeInteger(rowFields[9]),
					toSafeLong(rowFields[10]),
					toSafeLong(rowFields[11]),
					toSafeLong(rowFields[12])));
			
			DataFrame postsDF = sqlContext.createDataFrame(rowRdd, schema);
			postsDF.show(10, false);
			postsDF.printSchema();
			printSeparator();
			
			/*
			 * 1. Convert the DataFrame back to a JavaRDD<Row>
			 */			
			
			Instant startTS = Instant.now();
			JavaRDD<Row> postsRDD = postsDF.toJavaRDD();
			System.out.print(postsRDD.take(20));
			printSeparator();
			
			/*
			 * 2. Convert the `&lt;` and `&gt;` by their corresponding characters using RDD
			 */
			JavaRDD<Row> prettyPostsRDD = postsRDD.map(row -> RowFactory.create(row.get(0), row.get(1), row.get(2),
					row.getString(3).replace("&lt;", "<").replace("&gt;", ">"),
					row.get(4), row.get(5), row.get(6), row.get(7),
					row.getString(8).replace("&lt;", "<").replace("&gt;", ">"),
					row.get(9), row.get(10), row.get(11), row.get(12)));
			
			DataFrame prettyPostsDF = sqlContext.createDataFrame(prettyPostsRDD, postsDF.schema());
			Duration durationRDD = Duration.between(startTS, Instant.now());
			prettyPostsDF.show();
			printSeparator();
			
			/*
			 * 3. Convert the `&lt;` and `&gt;` by their corresponding characters using DataFrame functions
			 */
			Instant start = Instant.now();
			UDF1<String,String> prettyTagsFn = (str) -> str.replace("&lt;", "<").replace("&gt;", ">");
			sqlContext.udf().register("prettyTags", prettyTagsFn, DataTypes.StringType);	
			
			DataFrame altPrettyPostsDF = postsDF.select(col("commentCount"), col("lastActivityDate"), col("ownerUserId"), callUDF("prettyTags", col("body")).as("body"), col("score"), col("creationDate"), col("viewCount"), col("title"), callUDF("prettyTags", col("tags")).as("tags"), col("answerCount"), col("acceptedAnswerId"), col("postTypeId"), col("id"));
			Duration duration = Duration.between(start, Instant.now());
			altPrettyPostsDF.show();
			LOGGER.debug("duration UDF: {} // Duration RDD: {}", duration, durationRDD);
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
