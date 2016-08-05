package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeInteger;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeLong;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeTimestamp;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("DataFrames API Functions")
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
			 * 1. Find the question that has been active for the largest amount of time
			 */
			Row topActiveQuestion = postsDF.filter(postsDF.col("postTypeId").equalTo(1))
										.withColumn("activePeriod", datediff(postsDF.col("lastActivityDate"), postsDF.col("creationDate")))
										.sort(new Column("activePeriod").desc())
										.head();
			LOGGER.debug("Question active for the largest amount of time: {}", topActiveQuestion);
			printSeparator();
			
			/*
			 * 2. Show the body of the question that has been active for the largest amount of time, with un-escaped contents
			 */
			String unescapedTopActiveQuestionBody = topActiveQuestion
													.getString(3)
													.replace("&lt;", "<")
													.replace("&gt;", ">");
			LOGGER.debug("Unescaped body of question active for the largest amount of time: {}", unescapedTopActiveQuestionBody);
			printSeparator();
			
			/*
			 * 3. Find the average max and score of all the posts, and the total number of posts 
			 */
			DataFrame questionsStatsDF = postsDF.select(avg(postsDF.col("score")), max(postsDF.col("score")), count(postsDF.col("score")));
			questionsStatsDF.show(false);
			printSeparator();
			
			/*
			 * 4. Show maximum score of all user's questions, and an additional column that shows the difference between
			 *    this maximum and the current score.
			 */
			
			/* Window functions require a HiveContext (SQLContext is not enough) */
			HiveContext hiveContext = new HiveContext(sc);
			DataFrame postsOnHiveDF = hiveContext.createDataFrame(rowRdd, schema);
			
			DataFrame maxScoreOfUserQuestionsDF = postsOnHiveDF.filter(postsOnHiveDF.col("postTypeId").equalTo(1))
														.select(postsOnHiveDF.col("ownerUserId"), 
																postsOnHiveDF.col("acceptedAnswerId"), 
																postsOnHiveDF.col("score"), 
																max(postsOnHiveDF.col("score")).over(Window.partitionBy(postsOnHiveDF.col("ownerUserId"))).as("maxPerUser"))
														.withColumn("diffToMax", col("maxPerUser").minus(postsOnHiveDF.col("score")));
			maxScoreOfUserQuestionsDF.show(200, false);
			
			/*
			 * 5. Show for each question id of next and previous question by creation date
			 */
			DataFrame nextAndPrevQuestionByUserDF = postsOnHiveDF.filter(postsOnHiveDF.col("postTypeId").equalTo(1))
					.select(postsOnHiveDF.col("ownerUserId"), 
							postsOnHiveDF.col("id"), 
							postsOnHiveDF.col("creationDate"), 
							lag(postsOnHiveDF.col("id"), 1).over(Window.partitionBy(postsOnHiveDF.col("ownerUserId")).orderBy(postsOnHiveDF.col("creationDate"))).as("prev"),
					lead(postsOnHiveDF.col("id"), 1).over(Window.partitionBy(postsOnHiveDF.col("ownerUserId")).orderBy(postsOnHiveDF.col("creationDate"))).as("next"))
					.orderBy(postsOnHiveDF.col("ownerUserId"), postsOnHiveDF.col("id"));
			nextAndPrevQuestionByUserDF.show(200, false);			
			printSeparator();
			
			/*
			 * 6. Define a Spark UDF that count the number of tags in the tags column and show the number of tags for each of the questions.
			 */
			
			// we first register the UDF that return the number of `&lt;` found in a String
			// This can be done like this:
			/*
			UDF1<String,Integer> countTagsFn = (tags) -> StringUtils.countMatches(tags, "&lt;");
			sqlContext.udf().register("countTags", countTagsFn, DataTypes.IntegerType);			
			*/
			
			// or in a single step:
			sqlContext.udf().register("countTags", (String tags) -> StringUtils.countMatches(tags, "&lt;"), DataTypes.IntegerType);
			
			// Now we can use the function
			DataFrame numTagsPerQuestion = postsDF.filter(postsDF.col("postTypeId").equalTo(1))
												.select(postsDF.col("tags"), callUDF("countTags", postsDF.col("tags")).as("tagCnt"));
			numTagsPerQuestion.show(false);
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
