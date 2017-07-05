package org.joolzminer.examples.spark.java;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf()
								.setAppName("002-hello-filter")
								.setMaster("local[*]");
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(config)) {						
			Path sparkLicenseFile = Paths.get(System.getenv("SPARK_HOME"), "LICENSE");			
			JavaRDD<String> licLines = sparkContext.textFile(sparkLicenseFile.toString());	
			
			/* Option 1: inline lambda */
			JavaRDD<String> licLinesContainingBSD = licLines.filter(line -> line.contains("BSD"));			
			System.out.println("licLinesContainingBSD=" + licLinesContainingBSD.count());
			licLinesContainingBSD.foreach(line -> System.out.println("line with BSD: " + line));
			
			/* Option 2: named function */
			licLinesContainingBSD = licLines.filter(containsBSD);			
			System.out.println("licLinesContainingBSD=" + licLinesContainingBSD.count());
			
			/* Enhancement: parameterizing the string we use to filter the lines */
			try {
				Function<String,Boolean> containsStringBSD = containsString.call("BSD");
				licLinesContainingBSD = licLines.filter(containsStringBSD);			
				System.out.println("licLinesContainingBSD=" + licLinesContainingBSD.count());
			} catch (Exception e) {
				LOGGER.error("Could not use function factory", e);
				throw new RuntimeException(e);
			}
			
			/* An alternative approach for the parameterized solution: no need for try-catch when using old style named functions */
			licLinesContainingBSD = licLines.filter(containsStringFnFactory("BSD"));
			System.out.println("licLinesContainingBSD=" + licLinesContainingBSD.count());						
		}
	}	
	
	/* this function is not a java.util.Function but lambda notation is allowed */
	private static Function<String,Boolean> containsBSD = line -> line.contains("BSD");
	
	private static Function<String, Function<String,Boolean>> containsString = (String str) -> {
		return line -> line.contains(str);
	};
	
	private static Function<String,Boolean> containsStringFnFactory(String strToMatch) {
		return line -> line.contains(strToMatch);
	}
}
