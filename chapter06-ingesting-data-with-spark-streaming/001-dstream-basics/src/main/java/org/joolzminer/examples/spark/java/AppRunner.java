package org.joolzminer.examples.spark.java;

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeInteger;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeLong;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.toSafeTimestamp;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.joolzminer.examples.spark.java.utils.SafeConversions.*;


public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		SparkConf config = new SparkConf()
								.setAppName("DStream basics")
								.setMaster("local[*]");


		try (JavaStreamingContext jssc = new JavaStreamingContext(config, Durations.seconds(5))) {
			
			/*
			 * 0. Define the Input Sources: Set the watched directory for data ingestion
			 */
			JavaDStream<String> fileDStream = jssc.textFileStream(Paths.get("./src/main/resources", "streaming-dir").toString());
			
			
			JavaDStream<Order> orders = fileDStream.flatMap(line -> {
				String[] lineFields = line.split(",");
				try {
					List<Order> result = new ArrayList<>();
					Order order = new Order(toSafeTimestamp(lineFields[0]), toSafeLong(lineFields[1]), toSafeLong(lineFields[2]), lineFields[3], toSafeInteger(lineFields[4]), toSafeDouble(lineFields[5]), lineFields[6].equals("B"));
					result.add(order);
					return result;
				} catch (Exception e) {
					return new ArrayList<Order>(0);
				}
			});
			
			JavaPairDStream<Boolean, Long> numPerType = orders
															.mapToPair(order -> new Tuple2<>(order.isBuy(), 1L))
															.reduceByKey((c1, c2) -> c1 + c2);
			numPerType.foreachRDD(javaPairRDD -> {
				LOGGER.debug("Processing JavaPairRDD obtained from the JavaPairDStream");
				javaPairRDD.foreach(tuple2 -> {
					System.out.println(tuple2._1() + "=>" + tuple2._2());
				});
				printSeparator();
			});
			
			
			
			LOGGER.debug("Starting streaming process with 5 sec latency...");
			jssc.start();
			
			jssc.awaitTermination();
			printSeparator();
			
		} catch (Exception e) {
			LOGGER.error("An exception has been caught: ", e);
		}

	}
	
	private static void printSeparator() {
		System.out.println("===========================================================================");
	}
}
