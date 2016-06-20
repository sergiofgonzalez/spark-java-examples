package org.joolzminer.examples.spark.java;

import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("002-spark-basics-group-order")
								.setMaster("local[*]");

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {
			SQLContext sqlContext = new SQLContext(sparkContext);
			
			String githubLogFilename = "2015-03-01-0.json";
			String inputDir = "./src/main/resources";
			
			DataFrame githubLogDataFrame = sqlContext.read().json(Paths.get(inputDir, githubLogFilename).toString());
			DataFrame pushOperationsDataFrame = githubLogDataFrame.filter("type = 'PushEvent'");
						
			GroupedData pushByActorLogin = pushOperationsDataFrame.groupBy("actor.login");
			DataFrame pushOperationsByActorLogin = pushByActorLogin.count();
			
			DataFrame pushOperationsByActorLoginOrderedByCount = pushOperationsByActorLogin.orderBy(pushOperationsByActorLogin.col("count").desc());
			
			
			/* showing first 10 records in tabular form */
			pushOperationsByActorLoginOrderedByCount.show(10, false); /* false means do not truncate output */			
			printSeparator();
			
			/* 
			 	Manual display of all entries
			 	Note that the result is not strictly ordered, i guess that's because be haven't materialized the resulting
			 	collection. 
			 */
			pushOperationsByActorLoginOrderedByCount.foreach(new DataFrameRowPrinter());
			printSeparator();
			
			
			/* 
		 		However, if we materialize the results, then the ordering is correct.
			 */
			Arrays.stream(pushOperationsByActorLoginOrderedByCount.collect())
				.forEach(System.out::println);

			
		}
		
	}
	
	private static final void printSeparator() {
		System.out.println("=================================================================");
	}
}
