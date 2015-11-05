package org.joolzminer.examples.spark.java;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/* static imports for Functional programming */
import static java.util.stream.Collectors.*;
import static org.apache.spark.sql.functions.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		Instant startInstant = Instant.now();
		
		SparkConf config = new SparkConf();
		String dataDir, employeesFile, outDir;
		if (args.length == 0) {
			LOGGER.info("local[*] execution mode has been activated for this program");
			config.setAppName("005-spark-basics-cluster-ready")
				  .setMaster("local[*]");
			dataDir = "/tmp";
			outDir = "/tmp/out";
			employeesFile = "./src/main/resources/ghEmployees.txt";
		} else {
			LOGGER.info("spark-submit mode has been activated for this program");
			dataDir = args[0];
			employeesFile = args[1];
			outDir = args[2];
		}
		
		LOGGER.info("The github archive info will be taken from `{}/github-archive`", dataDir);
		LOGGER.info("The employees will be read from `{}/ghEmployees.txt`", employeesFile);
		LOGGER.info("Output (in JSON format) will be stored in `{}`", outDir);
		

		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {
			SQLContext sqlContext = new SQLContext(sparkContext);
					
			DataFrame githubLogDataFrame = sqlContext.read().json(Paths.get(dataDir, "github-archive", "*.json").toString());
			DataFrame pushOperationsDataFrame = githubLogDataFrame.filter("type = 'PushEvent'");
						
			GroupedData pushByActorLogin = pushOperationsDataFrame.groupBy("actor.login");
			DataFrame pushOperationsByActorLogin = pushByActorLogin.count();
			
			DataFrame pushOperationsByActorLoginOrderedByCount = pushOperationsByActorLogin.orderBy(pushOperationsByActorLogin.col("count").desc());
			
			
			/* filtering using values from file */
			Set<String> employees = Files.lines(Paths.get(employeesFile), Charset.defaultCharset())
										.collect(toSet());
			LOGGER.debug("{} employee(s) have been read from the file `{}`", employees.size(), employeesFile);
			
			Broadcast<Set<String>> employeesBroadcastObj = sparkContext.broadcast(employees);
			
			UDF1<String, Boolean> isEmployeeFn = (String login) -> employeesBroadcastObj.value().contains(login);
			
			
			sqlContext.udf().register("isEmployeeUDF", isEmployeeFn, DataTypes.BooleanType);			
			
			DataFrame employeesPushOperationsByActorLoginOrderedByCount = 
					pushOperationsByActorLoginOrderedByCount
							.filter(callUDF("isEmployeeUDF", col("login")));
																	
			employeesPushOperationsByActorLoginOrderedByCount.show(50);
			
			LOGGER.info("Writing results in JSON format to `{}`", outDir);
			employeesPushOperationsByActorLoginOrderedByCount.write().format("json").save(outDir);
		}

		LOGGER.info("Execution took: {} msecs", Duration.between(startInstant, Instant.now()));
	}
}
