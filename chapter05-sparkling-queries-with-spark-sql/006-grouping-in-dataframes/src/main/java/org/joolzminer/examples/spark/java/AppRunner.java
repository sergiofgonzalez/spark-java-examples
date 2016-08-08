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
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joolzminer.examples.spark.java.utils.MyDoubleAverageUDAF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
								.setAppName("Grouping data")
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
			 * 1. Find the number of posts per author, and show them in descending order
			 */			
			DataFrame byAuthorPostsDF = postsDF.groupBy(postsDF.col("ownerUserId")).count().orderBy(col("count").desc());
			byAuthorPostsDF.show();
			printSeparator();

			/*
			 * 2. Find the number of posts per author, tags and postTypeId and show them in descending order by ownerUserId
			 */
			DataFrame byAuthorTagAndPostTypeIdPostsDF = postsDF.groupBy(col("ownerUserId"), col("tags"), col("postTypeId")).count().orderBy(col("ownerUserId").desc());
			byAuthorTagAndPostTypeIdPostsDF.show(false);
			printSeparator();
			
			/*
			 * 3.  Find the last activity date by author (ownerUserId)
			 */
			DataFrame lastActivityByOwnerUserIdDF = postsDF.groupBy(col("ownerUserId")).agg(max(col("lastActivityDate"))).orderBy(col("ownerUserId").asc());
			lastActivityByOwnerUserIdDF.show(false);
			printSeparator();
			
			/*
			 * 4. Create a DataFrame with the last activity date and maximum score by author (ownerUserId)
			 */
			DataFrame lastActivityDateAndMaxScoreByAuthorDF = postsDF.groupBy(col("ownerUserId")).agg(max(col("lastActivityDate")), max(col("score"))).orderBy(col("ownerUserId").asc());
			lastActivityDateAndMaxScoreByAuthorDF.show(false);
			printSeparator();
			
			/*
			 * 5. Create a DataFrame with the last activity date and a column that shows whether the max score was greater than 5
			 */
			DataFrame lastActivityDateAndMaxScoreThresholdByAuthorDF = postsDF.groupBy(col("ownerUserId")).agg(max(col("lastActivityDate")), max(col("score").gt(5))).orderBy(col("ownerUserId").asc());
			lastActivityDateAndMaxScoreThresholdByAuthorDF.show(false);
			printSeparator();
			
			/*
			 * 6. Create a custom UDAF that computes the average. 
			 *    Test it by creating a DataFrame with a double column and applying the UDAF to it
			 */
			UserDefinedAggregateFunction myDoubleAverageUDAF = new MyDoubleAverageUDAF();
			UserDefinedAggregateFunction registeredUDAF = sqlContext.udf().register("myDoubleAverage", myDoubleAverageUDAF);
			JavaRDD<Double> numsRDD = sc.parallelize(Arrays.asList(0.0, 10.0));

			schemaFields = new ArrayList<StructField>() {{
				add(DataTypes.createStructField("num", DataTypes.DoubleType, true));
			}};
			
			schema = DataTypes.createStructType(schemaFields);
			
			JavaRDD<Row> rowNumsRdd = numsRDD.map(num -> RowFactory.create(num));
			
			DataFrame numsDF = sqlContext.createDataFrame(rowNumsRdd, schema);
			
			// You can use it as a regular UDF or use the apply method
			DataFrame aggregateDF = numsDF.agg(callUDF("myDoubleAverage", col("num")), registeredUDAF.apply(col("num")));
			aggregateDF.show();
			
			/*
			 * 7.
			 */
			
			// a. Select the posts from ownerUserId >= 13 and ownerUserId <= 15
			DataFrame subsetPostsDF = postsDF.where(col("ownerUserId").geq(13).and(col("ownerUserId").leq(15)));
			subsetPostsDF.show(false);
			
			// b. Count the posts aggregated by owner, tag and post type
			DataFrame subsetPostsAggDF = subsetPostsDF.groupBy(col("ownerUserId"), col("tags"), col("postTypeId")).count();
			subsetPostsAggDF.show(false);
			
			subsetPostsDF.groupBy(col("ownerUserId"), col("tags"), col("postTypeId")).count().show();
			
			/* using agg does not let you agg by several columns
			subsetPostsDF.groupBy(col("ownerUserId"), col("tags"), col("postTypeId")).agg(count(lit(1))).show();
			subsetPostsDF.groupBy(col("ownerUserId"), col("tags"), col("postTypeId")).agg(count(col("ownerUserId"))).show();
			*/
			
			// c. calculate the rollup, which returns:
			//    + the count as in subsection b.
			//    + subtotals per owner (where tags and post types are null)
			//    + subtotals per owner and tags (where postTypeId is null)
			//    + grand total (where all cols are null)
			DataFrame rollupDF = subsetPostsDF.rollup(col("ownerUserId"), col("tags"), col("postTypeId")).count();
			rollupDF.show();
			
			// d. calculate the cube, which returns all possible combinations (rollup assumes a hierarchy)
			DataFrame cubeDF = subsetPostsDF.cube(col("ownerUserId"), col("tags"), col("postTypeId")).count();
			cubeDF.show();
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
