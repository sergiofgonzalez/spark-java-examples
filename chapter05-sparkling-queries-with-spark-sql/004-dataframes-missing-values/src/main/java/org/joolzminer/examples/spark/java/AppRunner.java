package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeInteger;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeLong;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeTimestamp;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("DataFrames Missing Values")
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
			 * 1. Drop all the rows from the input dataset containing null or NaN in any of the columns
			 */			
			
			// drop in  any of the columns is drop() or drop("any")
			DataFrame cleanPostsDF = postsDF.na().drop();
			LOGGER.debug("Input rows: {} // Rows not containing any nulls: {}", postsDF.count(), cleanPostsDF.count());			
			printSeparator();
			
			/*
			 * 2. Drop all the rows from the input dataset containing null or NaN in all the columns
			 */
			cleanPostsDF = postsDF.na().drop("all");
			LOGGER.debug("Input rows: {} // drop(all): {}", postsDF.count(), cleanPostsDF.count());			
			printSeparator();
			
			/*
			 * 3. Drop all the rows with acceptedAnswerId is null
			 */
			cleanPostsDF = postsDF.na().drop(new String[] {"acceptedAnswerId"});
			LOGGER.debug("Input rows: {} // drop(acceptedAnswerId): {}", postsDF.count(), cleanPostsDF.count());			
			printSeparator();
			
			/*
			 * 4. Replace all null and NaN values with a constant value 0 for columns viewCount, answerCount and acceptedAnswerId
			 */
			DataFrame postsNoNullsDF = postsDF.na().fill(0, new String[] {"viewCount", "answerCount", "acceptedAnswerId"});
			postsNoNullsDF.show(false);
			printSeparator();
			
			/*
			 * 5. Replace null and NaN values with 0 for viewCount column, 0 for answerCount and -1 for acceptedAnswerId.
			 */
			Map<String,Object> readOnlyMappings = Collections.unmodifiableMap(new HashMap<String, Object>() {{
				put("viewCount", 0);
				put("answerCount", 0);
				put("acceptedAnswerId", -1);
			}});
			DataFrame correctedPostsDF = postsDF.na().fill(readOnlyMappings);
			correctedPostsDF.show();
			
			/*
			 * 6. Replace the records with id=1177 and acceptedAnswerId=1177 with 3000
			 */
			DataFrame origRecordsDF = postsDF.filter(col("id").equalTo(1177).or(col("acceptedAnswerId").equalTo(1177)));
			origRecordsDF.show();
			
			DataFrame fixedRecordsDF = postsDF.na().replace(new String[] {"id", "acceptedAnswerId"}, ImmutableMap.of(1177, 3000));
			fixedRecordsDF.filter(col("id").equalTo(3000).or(col("acceptedAnswerId").equalTo(3000))).show();
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
