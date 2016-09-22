package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
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

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("Saving and Loading Dataframes from S3")
								.setMaster("local[*]");

		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			/*
			 * Note that you must configure the access to S3 beforehand!!!
			 * 
			 * You can either do:
			 * 			sc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "");
			 * 			sc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "");
			 * 
			 * or set the environment variables:
			 * 			AWS_ACCESS_KEY_ID
			 * 			AWS_SECRET_ACCESS_KEY
			 */
			
			HiveContext hiveContext = new HiveContext(sc);
			
			/*
			 * 0. Load a DataFrame from the posts input data set, using the explicit schema method
			 */
			JavaRDD<String[]> postsInputDataFields = sc.textFile("s3n://awa-dev-data-processing-input/italianPosts.csv")
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
			
			DataFrame postsDF = hiveContext.createDataFrame(postsRowRdd, postsSchema);
			postsDF.show(10, false);
			postsDF.printSchema();
			printSeparator();
			
			/*
			 * 1. Write the DataFrame to S3 using Parquet format (both natively on S3 and on HDFS/S3)
			 */
			LOGGER.debug("Writing as a file on S3 fs");
			postsDF.write().format("parquet").mode(SaveMode.Overwrite).save("s3n://awa-dev-data-processing-output/posts.parquet");			

			LOGGER.debug("Writing as a file on HDFS over S3 fs");
			postsDF.write().format("parquet").mode(SaveMode.Overwrite).save("s3://awa-dev-data-hdfs-on-s3/posts.parquet");
			printSeparator();
			
									
			/*
			 * 4. Write the DataFrame in the Hive metastore and register it as a table
			 */
						
			// the file will be saved in Parquet format in hive.metastore.warehouse.dir property
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_parquet");
			postsDF.where(postsDF.col("id").equalTo(1166)).write().format("parquet").saveAsTable("temp_posts_parquet");
			hiveContext.sql("SELECT * FROM temp_posts_parquet").show();
			printSeparator();		
			
			/*
			 * 5. Create DataFrames from tables existing in the Hive metastore			
			 */
			DataFrame postsFromHiveDF = hiveContext.table("temp_posts_parquet");
			postsFromHiveDF.show();
			
			printSeparator();
			
			/*
			 * 6. Delete the recently created tables from the Hive metastore
			 */
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_parquet");
			printSeparator();
			
			
			/*
			 * 7. Load data from the Parquet file on S3, check that schema is correctly inferred and data is there
			 */
			DataFrame postsFromParquetFileDF = hiveContext.read().parquet("s3n://awa-dev-data-processing-output/posts.parquet");
			postsFromParquetFileDF.show();
			postsFromParquetFileDF.printSchema();
			printSeparator();
			
			/*
			 * 7. Load data from the Parquet file on HDFS-on-S3, check that schema is correctly inferred and data is there
			 */
			postsFromParquetFileDF = hiveContext.read().parquet("s3://awa-dev-data-hdfs-on-s3/posts.parquet");
			postsFromParquetFileDF.show();
			postsFromParquetFileDF.printSchema();
			printSeparator();
			
			
			/*
			 * 2. Write the DataFrame as a MySQL table on a non-existing table
			 */
			postsDF.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts", new Properties() {{
				setProperty("user", "root");
				setProperty("password", "the-password-here");
			}});
			printSeparator();
			
			/*
			 * 3. Append the DataFrame on a pre-existing MySQL Table
			 * (remember to create the empty table with the appropriate structure beforehand)
			 */
			postsDF.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts", new Properties() {{
				setProperty("user", "root");
				setProperty("password", "the-password-here");
			}});
			printSeparator();
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
