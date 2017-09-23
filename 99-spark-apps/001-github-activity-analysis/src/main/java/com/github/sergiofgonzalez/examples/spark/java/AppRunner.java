package com.github.sergiofgonzalez.examples.spark.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;
import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;
import static com.github.sergiofgonzalez.examples.spark.java.utils.SparkApplicationTemplate.*;

public class AppRunner {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	private static final int GZIP_BUFFER_SIZE_IN_BYTES = 1024;

	public static void main(String[] args) {		

		/* 
		 * Running a Spark application using the SparkApplicationTemplate:
		 *   + withSparkDo is given a configuration object you use to
		 *     configure the application you want to run:
		 *     + withName               : provides the application name
		 *     + withPropertiesFile     : provides the application-level properties (if any)
		 *     + withExtraConfigVars    : used to pass command line args (if any) to the template to override config properties
		 *     + doing                  : used to pass the actual Spark application to execute
		 *     + afterSparkInitializedDo: initialization hook (if any)
		 *     + afterProcessingComplete: finalization hook (if any)
		 */
		withSparkDo(cfg -> cfg
						.withName("GithubDayAnalysis")
						.withPropertiesFile("config/spark-job.conf")
						.withExtraConfigVars(args)
						.doing(githubAnalysisApp)
						.afterSparkInitializedDo(awsAwareInitializationHook)
						.submit());		
	}
	

	/* if aws.filesUploadedToS3 flag is enabled, configure s3a file system */
	private static final BiConsumer<SparkSession, JavaSparkContext> awsAwareInitializationHook = (spark, sparkContext) -> {
		if (wconf().get("aws.filesUploadedToS3").equalsIgnoreCase("true")) {
			sparkContext.hadoopConfiguration().set("fs.s3a.access.key", wconf().get("aws.access_key_id"));
			sparkContext.hadoopConfiguration().set("fs.s3a.secret.key", wconf().get("aws.secret_access_key"));			
		}
	};
	
	
	/* The Spark application to execute */
	private static final BiConsumer<SparkSession, JavaSparkContext> githubAnalysisApp = (spark, sparkContext) -> {

		printConfigValues(Arrays.asList("dummy"));
			

		boolean filesUploadedToS3 = wconf().get("aws.filesUploadedToS3").equalsIgnoreCase("true")? true : false;
		String filesPath;
		if (!filesUploadedToS3) {
			String downloadPath = wconf().get("files.download_path");
			prepareInputData(wconf().get("files.input", true), downloadPath);
			filesPath = Paths.get(downloadPath, "*.json").toString();
			LOGGER.debug("Files will be processed in local file system: {}", filesPath);			
		} else {
			filesPath = String.format("s3a://%s/%s/*.json", wconf().get("aws.s3.bucket"), wconf().get("aws.s3.base_object_key"));
			LOGGER.debug("Files will be processed from S3 using s3a protocol: {}", filesPath);			
		}

		
		/* load data into a Dataset */
		Dataset<Row> githubLogData = spark.read().json(filesPath).cache();
		
		/* filter `PushEvent` */
		Dataset<Row> pushData = githubLogData.filter("type = 'PushEvent'").cache();
		
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
		String namesFilePath = wconf().get("files.login_names_path");
		Set<String> namesToMatch = getNamesToMatchFromFile(namesFilePath);
		System.out.println("Names loaded in Java set: " + namesToMatch.size());
		
		/* Now we create a Broadcast variable to distribute it efficiently to Executors */
		Broadcast<Set<String>> bcNamesToMatch = sparkContext.broadcast(namesToMatch);
		
		/* Now we define a UDF so that we can use it in a filter */
		spark.udf().register("isNameInListUDF", name -> bcNamesToMatch.value().contains(name), DataTypes.BooleanType);
		
		/* and use it in a filter expression */
		Dataset<Row> filteredOrderedData = pushByLoginOrderedData.filter(callUDF("isNameInListUDF", pushByLoginOrderedData.col("login")));
		filteredOrderedData.show(false);	
		
		filteredOrderedData.foreach(record -> {
			/* When running on a cluster, this will print some config vars on the worker logs (won't be visible on the master) */
			printConfigValues(Arrays.asList("wconf_app_properties", "dummy", "spark.master"));
			System.out.println(record);	
		});
	};

		
	private static void prepareInputData(List<String> filesToDownload, String outputPath) {
		LOGGER.debug("Preparing input data files to be analyzed");
		filesToDownload.stream()
			.parallel()
			.map(file -> {
				LOGGER.debug("About to process {}", file);
				try {
					return new URL(file);
				} catch (MalformedURLException e) {
					LOGGER.error("Could not create URL from {}", file, e);
					throw new IllegalArgumentException("Could not create URL", e);
				}
			})
			.forEach(fileUrl -> {
				Path outFilePath = Paths.get(outputPath, fileUrl.getFile());

				if (!Files.exists(outFilePath)) {
					try {
						LOGGER.debug("Downloading {} to {}", fileUrl, outFilePath.toString());
						FileUtils.copyURLToFile(fileUrl, outFilePath.toFile());
						
						LOGGER.debug("Unzipping {}", outFilePath.toString());
						gunzipFile(outFilePath);											
						
						LOGGER.debug("Processing completed for {}", fileUrl.toString());					
					} catch (IOException e) {
						LOGGER.error("Could not copy file from URL {}: fileUrl", e);
						throw new IllegalArgumentException("Could not copy file from URL", e);
					}					
				} else {
					LOGGER.debug("File {} already exists. It will not be redownloaded", outFilePath);
				}
			});
	}
	
	@SuppressWarnings("serial")
	private static Set<String> getNamesToMatchFromFile(String namesFilePath) {
		return new HashSet<String>() {{
			try (BufferedReader bufferedReader = classpathAwareBufferedReaderFactory(namesFilePath)) {
				String line = bufferedReader.readLine();
				while (line != null) {
					add(line);
					line = bufferedReader.readLine();
				}
			} catch (IOException e) {
				LOGGER.error("Could not read file: {}", namesFilePath, e);
				throw new RuntimeException(e);
			}
		}};		
	}
	
	private static BufferedReader classpathAwareBufferedReaderFactory(String namesFilePath) throws IOException {
		if (namesFilePath.startsWith("classpath://")) {
			String internalPath = namesFilePath.substring("classpath://".length());
			return new BufferedReader(new InputStreamReader(AppRunner.class.getClassLoader().getResourceAsStream(internalPath), StandardCharsets.UTF_8));			
		} else {
			return Files.newBufferedReader(Paths.get(namesFilePath), StandardCharsets.UTF_8);
		}		
	}

		
//	private static void prepareInputData(List<String> filesToDownload, String outputPath) {
//		LOGGER.debug("Starting preparation of input data for analysis");;
//		filesToDownload.stream()
//			.map(file -> {
//				LOGGER.debug("About to process {}", file);
//				try {
//					return new URL(file);
//				} catch (MalformedURLException e) {
//					LOGGER.error("Could not create URL from {}", file, e);
//					throw new IllegalArgumentException("Could not create URL", e);
//				}
//			})
//			.forEach(fileUrl -> {
//				Path outFilePath = Paths.get(outputPath, fileUrl.getFile());
//				try {
//					FileUtils.copyURLToFile(fileUrl, outFilePath.toFile());
//					Path gunzippedFilePath = gunzipFile(outFilePath);
//					uploadFileToS3(gunzippedFilePath, wconf().get("aws.s3.bucket"), wconf().get("aws.s3.object_key"));
//					LOGGER.debug("Processing completed for {}", fileUrl.toString());					
//				} catch (IOException e) {
//					LOGGER.error("Could not copy file from URL {}: fileUrl", e);
//					throw new IllegalArgumentException("Could not copy file from URL", e);
//				}
//			});
//	}
	
	private static Path gunzipFile(Path gzippedFile) {
		Path gunzippedFile = gzippedFile.resolveSibling(gzippedFile.getFileName().toString().substring(0, FilenameUtils.indexOfExtension(gzippedFile.getFileName().toString())));
		
		byte[] buffer = new byte[GZIP_BUFFER_SIZE_IN_BYTES];
		try (GZIPInputStream gzIn = new GZIPInputStream(Files.newInputStream(gzippedFile, StandardOpenOption.READ));
				OutputStream out = Files.newOutputStream(gunzippedFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			
			int len;
			while ((len = gzIn.read(buffer))> 0) {
				out.write(buffer, 0, len);
			}			
		} catch (IOException e) {
			LOGGER.error("Could not uncompress file {}", gzippedFile, e);
			throw new IllegalArgumentException("Could not uncompress file", e);
		}
		return gunzippedFile;
	}
	
//	private static void uploadFileToS3(Path inputFile, String s3Bucket, String s3BaseObjectKey) {
//		LOGGER.debug("Uploading {} to s3://{}/{}", inputFile, s3Bucket, s3BaseObjectKey);
//		Path objectKey = Paths.get(s3BaseObjectKey, inputFile.getFileName().toString());
//		
//		final AmazonS3 client = AmazonS3ClientBuilder
//									.standard()
//									.withCredentials(new ProfileCredentialsProvider(wconf().get("aws.profile")))
//									.withRegion(wconf().get("aws.region"))
//									.build();
//				
//		
//		TransferManager transferManager = TransferManagerBuilder.standard()
//											.withExecutorFactory(() -> Executors.newFixedThreadPool(Integer.parseInt(wconf().get("aws.s3.num_threads"))))
//											.withMinimumUploadPartSize(Long.parseLong(wconf().get("aws.s3.min_upload_part_size")))
//											.withS3Client(client)
//											.build();
		
		/* previous approach can be simplified using the default values */
//		TransferManager transferManager = TransferManagerBuilder.standard()
//											.withS3Client(client)
//											.build();
		
//		Upload upload = transferManager.upload(s3Bucket, objectKey.toString(), inputFile.toFile());
//		
//		try {
//			upload.waitForCompletion();
//			LOGGER.debug("Completed s3://{}/{}", s3Bucket, objectKey.toString());
//		} catch (AmazonClientException | InterruptedException e) {
//			LOGGER.error("Could not complete upload for {} into s3://{}/{}. You might need to abort the multipart upload using the CLI.", inputFile, s3Bucket, objectKey, e);			
//			throw new IllegalStateException("Could not complete upload", e);			
//		}
//	}
	
	/* this can be used to validate that workers get the same config as the driver and master */
	private static final void printConfigValues(List<String> configKeys) {
		System.out.println("\n=================================================================================");
		System.out.println("Environment:" + System.getenv());
		System.out.println("System Properties: " + System.getProperties());
		configKeys.stream()
			.forEach(configKey -> System.out.println(String.format("wconf =>: %s=%s", configKey, wconf().get(configKey))));
		System.out.println("\n=================================================================================");		
	}

}
