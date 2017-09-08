package com.github.sergiofgonzalez.examples.spark.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	private static final String DOWNLOAD_PATH = "/tmp/github-data"; 
	private static final String NAMES_FILE_PATH = "./src/main/resources/login-names-to-match.txt";
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		Instant start = Instant.now();
		
		SparkConf config = new SparkConf()
								.setAppName("001-github-activity-analysis-v1")
								.setMaster("local[*]");
		
		SparkSession spark = SparkSession
								.builder()
								.config(config)
								.getOrCreate();
		
		
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext())) {
			/* load data into a Dataset */
			String inputPath = Paths.get(DOWNLOAD_PATH, "*.json").toString();
			Dataset<Row> githubLogData = spark.read().json(inputPath);
			
			/* filter `PushEvent` */
			Dataset<Row> pushData = githubLogData.filter("type = 'PushEvent'");
			
			/* validate what we have so far */
			pushData.printSchema();
			System.out.println("Count events    : " + githubLogData.count());
			System.out.println("Count pushEvents: " + pushData.count());			
			pushData.show(5, false);
			
			/* group by login */
			Dataset<Row> pushByLoginData = pushData.groupBy("actor.login").count();
			pushByLoginData.show(5, false);
			
			/* group by login (sorted desc) */
			Dataset<Row> pushByLoginOrderedData = pushByLoginData.orderBy(pushByLoginData.col("count").desc());
			pushByLoginOrderedData.show(5, false);
			
			/* now match only a handful of custom login names */
			Set<String> namesToMatch = new HashSet<String>() {{
				try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(NAMES_FILE_PATH), Charset.forName("utf8"))) {
					String line = bufferedReader.readLine();
					while (line != null) {
						add(line);
						line = bufferedReader.readLine();
					}
				} catch (IOException e) {
					LOGGER.error("Could not read file: {}", NAMES_FILE_PATH, e);
					throw new RuntimeException(e);
				}
			}};
			System.out.println("Names loaded in Java set: " + namesToMatch.size());
			
			/* Now we create a Broadcast variable to distribute it efficiently to Executors */
			Broadcast<Set<String>> bcNamesToMatch = sparkContext.broadcast(namesToMatch);
			
			/* Now we define a UDF so that we can use it in a filter */
			spark.udf().register("isNameInListUDF", name -> bcNamesToMatch.value().contains(name), DataTypes.BooleanType);

			
			/* and use it in a filter expression */
			Dataset<Row> filteredOrderedData = pushByLoginOrderedData.filter(callUDF("isNameInListUDF", pushByLoginOrderedData.col("login")));
			filteredOrderedData.show(false);
			
			
		} catch (Exception e) {
			LOGGER.error("Application failed:", e);			
		} finally {
			spark.stop();
			Duration duration = Duration.between(start, Instant.now());
			LOGGER.info("Processing took {}", duration);
		}
	}	
}
