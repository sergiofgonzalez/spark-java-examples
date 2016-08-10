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
import org.apache.spark.sql.hive.HiveContext;
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

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("SQL and HiveQL with DataFrames")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			SQLContext sqlContext = new SQLContext(sc);
			
			/*
			 * 0. Load a DataFrame from the posts input data set, using the explicit schema method
			 */
			Path postsInputDataPath = Paths.get("./src/main/resources", "italianPosts.csv");
			JavaRDD<String[]> postsInputDataFields = sc.textFile(postsInputDataPath.toString())
													.map(line -> line.split("~"));
			

			List<StructField> postsSchemaFields = new ArrayList<StructField>() {{
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
			
			StructType postsSchema = DataTypes.createStructType(postsSchemaFields);
			
			JavaRDD<Row> postsRowRdd = postsInputDataFields.map(rowFields -> RowFactory.create(
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
			
			DataFrame postsDF = sqlContext.createDataFrame(postsRowRdd, postsSchema);
			postsDF.show(10, false);
			postsDF.printSchema();
			printSeparator();
			
			/*
			 * 1. Load a DataFrame from the votes input data set, using the explicit schema method
			 */
			Path votesInputDataPath = Paths.get("./src/main/resources", "italianVotes.csv");
			JavaRDD<String[]> votesInputDataFields = sc.textFile(votesInputDataPath.toString())
													.map(line -> line.split("~"));
			

			List<StructField> votesSchemaFields = new ArrayList<StructField>() {{
				add(DataTypes.createStructField("id", DataTypes.LongType, false));
				add(DataTypes.createStructField("postId", DataTypes.LongType, false));
				add(DataTypes.createStructField("voteTypeId", DataTypes.LongType, false));
				add(DataTypes.createStructField("creationDate", DataTypes.TimestampType, false));
			}};
			
			StructType votesSchema = DataTypes.createStructType(votesSchemaFields);
			
			JavaRDD<Row> votesRowRdd = votesInputDataFields.map(rowFields -> RowFactory.create(
					toSafeLong(rowFields[0]),
					toSafeLong(rowFields[1]),
					toSafeLong(rowFields[2]),
					toSafeTimestamp(rowFields[3])));
			
			DataFrame votesDF = sqlContext.createDataFrame(votesRowRdd, votesSchema);
			votesDF.show(10, false);
			votesDF.printSchema();
			printSeparator();			
			
			/*
			 * 2. Register the DataFrames as temporary tables in Spark and perform simple selects on them
			 */
			postsDF.registerTempTable("temp_posts");
			votesDF.registerTempTable("temp_votes");
			
			sqlContext.sql("SELECT * FROM temp_posts").show();
			printSeparator();
			
			sqlContext.sql("SELECT * FROM temp_votes").show();
			printSeparator();			

			/*
			 * 3. Register the DataFrames as permanent tables in Spark and perform simple selects on them
			 */
			
			// This requires a HiveContext
			HiveContext hiveContext = new HiveContext(sc);

			/* This will only work the first time 
			hiveContext.createDataFrame(postsRowRdd, postsSchema).write().saveAsTable("posts");
			hiveContext.createDataFrame(votesRowRdd, votesSchema).write().saveAsTable("votes");
			*/
			
			hiveContext.sql("SELECT * FROM posts").show();
			printSeparator();
			
			hiveContext.sql("SELECT * FROM votes").show();
			printSeparator();		
			
			/*
			 * 4. Perform a join using SQL between the two tables
			 */
			hiveContext.sql("SELECT a.*, b.* FROM posts a, votes b WHERE a.id = b.postId").show();
			printSeparator();
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
