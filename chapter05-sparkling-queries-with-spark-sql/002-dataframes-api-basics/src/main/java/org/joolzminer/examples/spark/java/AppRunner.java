package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
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
								.setAppName("DataFrames API Basics")
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
			 * 1. Select the columns id and body from the DataFrame using Strings
			 */
			DataFrame idAndBodiesDF = postsDF.select("id", "body");
			idAndBodiesDF.show(10, false);
			idAndBodiesDF.printSchema();
			printSeparator();
			
			/*
			 * 2. Select the columns id and body from the DataFrame using Strings
			 */
			idAndBodiesDF = postsDF.select(postsDF.col("id"), postsDF.col("body"));
			idAndBodiesDF.show(10, false);
			idAndBodiesDF.printSchema();
			printSeparator();
			
			/*
			 * 3. Select all the columns except body
			 */
			DataFrame postsWithoutBodyDF = postsDF.drop(postsDF.col("body"));
			postsWithoutBodyDF.show(10, false);
			postsWithoutBodyDF.printSchema();
			printSeparator();
			
			/*
			 * 4. Filter all records containing the word `Italiano` in its body
			 */
			DataFrame postsContainingItalianoDF = idAndBodiesDF.filter(idAndBodiesDF.col("body").contains("Italiano"));
			postsContainingItalianoDF.show(10, false);
			postsContainingItalianoDF.printSchema();
			LOGGER.debug("Number of records containing `Italiano`: {}", postsContainingItalianoDF.count());
			printSeparator();
			
			/*
			 * 5. Filter all the questions that do not have an accepted answer
			 */
			DataFrame questionsWithoutAnswerDF = postsDF.filter(postsDF.col("postTypeId").equalTo(1).and(postsDF.col("acceptedAnswerId").isNull()));
			questionsWithoutAnswerDF.show(false);
			questionsWithoutAnswerDF.printSchema();
			LOGGER.debug("Number of questions without accepted answer: {}", questionsWithoutAnswerDF.count());
			printSeparator();
			
			/*
			 * 6. Rename column `ownerUserId` as `owner`
			 */
			DataFrame postsWithColumnRenamedDF = postsDF.withColumnRenamed("ownerUserId", "owner");
			postsWithColumnRenamedDF.printSchema();
			printSeparator();
			
			/*
			 * 7. Add a new column consisting in the metric `ratio=viewCount/score` and print those records
			 * whose metric is less than 35.
			 */
			DataFrame enhancedPostsDF = postsDF
											.filter(postsDF.col("postTypeId").equalTo(1))
											.withColumn("ratio", postsDF.col("viewCount").divide(postsDF.col("score")))
											.where(new Column("ratio").lt(35));			
			enhancedPostsDF.show(false);
			
			// Strings can also be used as expressions when needed
			enhancedPostsDF = postsDF
					.filter(postsDF.col("postTypeId").equalTo(1))
					.withColumn("ratio", postsDF.col("viewCount").divide(postsDF.col("score")))
					.where("ratio < 35");
			enhancedPostsDF.show(false);			
			printSeparator();
			
			/*
			 * 8. Sort and show the 10 most recently modified questions
			 */
			DataFrame tenMostRecentlyModifiedQuestions = postsDF
															.filter(postsDF.col("postTypeId").equalTo(1))
															.sort(postsDF.col("lastActivityDate").desc())
															.limit(10);
			tenMostRecentlyModifiedQuestions.show(false);			
			
			// Sort and orderBy are synonyms
			tenMostRecentlyModifiedQuestions = postsDF
					.filter(postsDF.col("postTypeId").equalTo(1))
					.orderBy(postsDF.col("lastActivityDate").desc())
					.limit(10);			
			tenMostRecentlyModifiedQuestions.show(false);
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
