package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joolzminer.examples.spark.java.domain.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import static org.joolzminer.examples.spark.java.domain.utils.EmployeeComparatorFactory.*;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("007-sort-with-complex-keys")
								.setMaster("local[*]");


		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			JavaPairRDD<Employee, String> employeesPairRDD = sc.parallelizePairs(Arrays.asList(
					new Tuple2<>(new Employee("Adrian", "Fernandez"), "Training"),
					new Tuple2<>(new Employee("Inma", "Bermejo"), "Administration"),
					new Tuple2<>(new Employee("Sergio", "Fernandez"), "Development")));
			
			System.out.println(
					employeesPairRDD
						.sortByKey(byFirstName())
						.collect()
			);
			
			System.out.println(
					employeesPairRDD
						.sortByKey(byLastName())
						.collect()
			);
						
		}
	}

	
	
	public static void printSeparator() {
		System.out.println("=================================================================================");
	}
}
