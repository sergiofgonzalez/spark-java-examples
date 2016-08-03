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
								.setAppName("Creating Dataframes from RDDs")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			SQLContext sqlContext = new SQLContext(sc);
			
			/*
			 * 0. Load the input data set, identifying each the fields
			 */
			Path inputDataPath = Paths.get("./src/main/resources", "italianPosts.csv");
			JavaRDD<String[]> inputDataFields = sc.textFile(inputDataPath.toString())
													.map(line -> line.split("~"));
			
			LOGGER.debug("Number of rows: {}", inputDataFields.count());
			LOGGER.debug("Number of fields in each row: {}", inputDataFields.first().length);
			LOGGER.debug("First line (String[]): {}", Arrays.asList(inputDataFields.first()));
			printSeparator();
			
			/*
			 * 1. Create an RDD in which every row is an instance of the Post class, using flatMap
			 */			
			JavaRDD<Post> postsRdd = inputDataFields.flatMap(fields -> {
				List<Post> posts = new ArrayList<>();
				try {
					Post post = new Post(fields);
					posts.add(post);
				} catch (Exception e) {
					LOGGER.warn("Could not create Post from {}", Arrays.asList(fields));
				}
				return posts;
			});
						
			LOGGER.debug("Number of posts: {}", postsRdd.count());
			LOGGER.debug("First line (Post): {}", postsRdd.first());
			printSeparator();
			
			/*
			 * 2. Create an RDD in which every row is an instance of the Post class, using map
			 */			
			postsRdd = inputDataFields.map(fields -> {				
				try {
					return new Post(fields);
				} catch (Exception e) {
					LOGGER.warn("Could not create Post from {}", Arrays.asList(fields));
					throw new IllegalArgumentException("Could not create post from fields");
				}
			});

			LOGGER.debug("Number of posts: {}", postsRdd.count());
			LOGGER.debug("First line (Post): {}", postsRdd.first());	
			printSeparator();
			
			/*
			 * 3. Create a DataFrame from the RDD in which every item is a Post instance
			 */				
			DataFrame postsDf = sqlContext.createDataFrame(postsRdd, Post.class);
			postsDf.show(10, false);
			postsDf.printSchema();
			printSeparator();	
			
			/*
			 * 4. Create a DataFrame from the RDD by explicitly specifying a schema
			 */
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
			
			DataFrame postsWithSchemaDF = sqlContext.createDataFrame(rowRdd, schema);
			postsWithSchemaDF.show(10, false);
			postsWithSchemaDF.printSchema();
			printSeparator();
			
			/*
			 * 5. Obtain the columns and the associated data types from the schema
			 */
			LOGGER.debug("Columns of the DataFrame: {}", String.join(", ", postsWithSchemaDF.columns()));
			Arrays.stream(postsWithSchemaDF.dtypes())
				.forEach(typeTuple2 -> {
					System.out.println("schema field name: " + typeTuple2._1() + "; schema field type: " + typeTuple2._2());
				});
			printSeparator();
		
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
