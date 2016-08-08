package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
								.setAppName("Joining DataFrames")
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
			 * 2. Join the posts and votes DataFrames by the `posts.id` and `votes.postId`
			 */
			
			// this is an inner join: the resulting DF will contain all possible values from the first and second DFs
			// No keys will be found for keys that exist only in one of the DataFrames
			
			DataFrame innerJoinPostsAndVotesDF = postsDF.join(votesDF, postsDF.col("id").equalTo(votesDF.col("postId")));
			innerJoinPostsAndVotesDF.show();
			
			// this is an outer join: the resulting DF will contain all possible values with null values
			// for keys that exist only in one of the DFs
			DataFrame outerJoinPostsAndVotesDF = postsDF.join(votesDF, postsDF.col("id").equalTo(votesDF.col("postId")), "outer");
			outerJoinPostsAndVotesDF.show();
			
			/*
			 * 3.
			 */
			JavaRDD<String[]> spanishNumsRDD = sc.parallelize(Arrays.asList(new String[]{"1", "uno"}, new String[]{"2", "dos"}, new String[] {"3", "tres"}, new String[] {"2", "dos"}));
			List<StructField> spanishNumsSchemaFields = new ArrayList<StructField>() {{
				add(DataTypes.createStructField("num", DataTypes.IntegerType, false));
				add(DataTypes.createStructField("spanish", DataTypes.StringType, false));
			}};
			
			StructType spanishSchema = DataTypes.createStructType(spanishNumsSchemaFields);
			
			JavaRDD<Row> spanishRowsRdd = spanishNumsRDD.map(rowFields -> RowFactory.create(
					toSafeInteger(rowFields[0]),
					rowFields[1]));
			
			DataFrame spanishDF = sqlContext.createDataFrame(spanishRowsRdd, spanishSchema);
			spanishDF.show();
			
			JavaRDD<String[]> englishNumsRDD = sc.parallelize(Arrays.asList(new String[]{"1", "one"}, new String[]{"2", "two"}));
			List<StructField> englishNumsSchemaFields = new ArrayList<StructField>() {{
				add(DataTypes.createStructField("num", DataTypes.IntegerType, false));
				add(DataTypes.createStructField("english", DataTypes.StringType, false));
			}};
			
			StructType englishSchema = DataTypes.createStructType(englishNumsSchemaFields);
			
			JavaRDD<Row> englishRdd = englishNumsRDD.map(rowFields -> RowFactory.create(
					toSafeInteger(rowFields[0]),
					rowFields[1]));
			
			DataFrame englishDF = sqlContext.createDataFrame(englishRdd, englishSchema);
			englishDF.show();
			
			DataFrame innerJoinDF = spanishDF.join(englishDF, "num");
			innerJoinDF.show(); 
			printSeparator();
			
			DataFrame leftOuterJoinDF = spanishDF.join(englishDF, spanishDF.col("num").equalTo(englishDF.col("num")), "left_outer");
			leftOuterJoinDF.show();
			printSeparator();
			
			DataFrame rightOuterJoinDF = spanishDF.join(englishDF, spanishDF.col("num").equalTo(englishDF.col("num")), "right_outer");
			rightOuterJoinDF.show();
			printSeparator();

			DataFrame fullOuterJoinDF = spanishDF.join(englishDF, spanishDF.col("num").equalTo(englishDF.col("num")), "full_outer");
			fullOuterJoinDF.show();
			printSeparator();
			
			DataFrame leftSemiJoinDF = spanishDF.join(englishDF, spanishDF.col("num").equalTo(englishDF.col("num")), "leftsemi");
			leftSemiJoinDF.show();
			printSeparator();			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
