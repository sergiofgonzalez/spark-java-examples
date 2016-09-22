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
import java.nio.file.Path;
import java.nio.file.Paths;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("Saving and Loading Dataframes")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			HiveContext hiveContext = new HiveContext(sc);
			
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
			
			DataFrame postsDF = hiveContext.createDataFrame(postsRowRdd, postsSchema);
			postsDF.show(10, false);
			postsDF.printSchema();
			printSeparator();
			
			
			/*
			 * 1. Write the DataFrame using JSON, ORC and Parquet format
			 */
			postsDF.write().format("json").mode(SaveMode.Overwrite).save(Paths.get("./src/main/resources/dataframes-data/", "posts.json").toString());
			postsDF.write().format("orc").mode(SaveMode.Overwrite).save(Paths.get("./src/main/resources/dataframes-data/", "posts.orc").toString());
			postsDF.write().format("parquet").mode(SaveMode.Overwrite).save(Paths.get("./src/main/resources/dataframes-data/", "posts.parquet").toString());			
			printSeparator();
			
			/*
			 * 2. Write the DataFrame as a MySQL table on a non-existing table
			 */
			postsDF.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts", new Properties() {{
				setProperty("user", "root");
				setProperty("password", "passwd");
			}});
			printSeparator();
			
			/*
			 * 3. Write the DataFrame as a MySQL table on an empty table
			 */
			postsDF.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts_empty", new Properties() {{
				setProperty("user", "root");
				setProperty("password", "passwd");
			}});
			printSeparator();
			
			/*
			 * 4. Write the DataFrame using the JSON format and register it as a table
			 */
			
			// the file will be saved in JSON format in hive.metastore.warehouse.dir property
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_json");
			postsDF.write().format("json").saveAsTable("temp_posts_json");
			hiveContext.sql("SELECT * FROM temp_posts_json").show();
			
			// the file will be saved in ORC format in hive.metastore.warehouse.dir property
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_orc");
			postsDF.where(postsDF.col("id").equalTo(1165)).write().format("orc").saveAsTable("temp_posts_orc");
			hiveContext.sql("SELECT * FROM temp_posts_orc").show();
			
			// the file will be saved in Parquet format in hive.metastore.warehouse.dir property
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_parquet");
			postsDF.where(postsDF.col("id").equalTo(1166)).write().format("parquet").saveAsTable("temp_posts_parquet");
			hiveContext.sql("SELECT * FROM temp_posts_parquet").show();
			printSeparator();		
			
			/*
			 * 5. Create DataFrames from tables existing in the Hive metastore			
			 */
			DataFrame postsFromHiveDF = hiveContext.table("temp_posts_json");
			postsFromHiveDF.show();
			
			postsFromHiveDF = hiveContext.table("temp_posts_orc");
			postsFromHiveDF.show();
			
			postsFromHiveDF = hiveContext.table("temp_posts_parquet");
			postsFromHiveDF.show();
			
			printSeparator();
			
			/*
			 * 6. Delete the recently created tables from the Hive metastore
			 */
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_json");
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_parquet");
			hiveContext.sql("DROP TABLE IF EXISTS temp_posts_orc");
			printSeparator();
			
			/*
			 * 7. Load data from relational db using JDBC
			 */
			DataFrame postsFromJdbcDF = hiveContext.read().jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts", new Properties() {{
				setProperty("user", "root");
				setProperty("password", "passwd");
			}});
			postsFromJdbcDF.show();
			
			// You can also specify a predicate to load only a subset of the records
			postsFromJdbcDF = hiveContext.read().jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts", new String[] { "id = 1181"}, new Properties() {{
				setProperty("user", "root");
				setProperty("password", "passwd");
			}});
			postsFromJdbcDF.show();
			printSeparator();
			
			/*
			 * 8. Refresh the data in the db and check data is refreshed
			 */
			postsFromJdbcDF = hiveContext.read().jdbc("jdbc:mysql://localhost:3306/mysparkdfs", "posts", new Properties() {{
				setProperty("user", "root");
				setProperty("password", "passwd");
			}});
			postsFromJdbcDF.show();
			printSeparator();
			
			/*
			 * 9. Load data from the JSON file, check that schema is correctly inferred
			 */
			DataFrame postsFromJsonFileDF = hiveContext.read().json(Paths.get("./src/main/resources/dataframes-data", "posts.json").toString());
			postsFromJsonFileDF.show();
			postsFromJsonFileDF.printSchema();
			printSeparator();
			
			/*
			 * 10. Load data from the Parquet file, check that schema is correctly inferred
			 */
			DataFrame postsFromParquetFileDF = hiveContext.read().parquet(Paths.get("./src/main/resources/dataframes-data", "posts.parquet").toString());
			postsFromParquetFileDF.show();
			postsFromParquetFileDF.printSchema();
			printSeparator();
			
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
