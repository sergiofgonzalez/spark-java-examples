package com.github.sergiofgonzalez.examples.spark.java.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;

public class S3FileUploder {	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(S3FileUploder.class);
	
	private static final int GZIP_BUFFER_SIZE_IN_BYTES = 1024;
		
	
	public static void main(String[] args) {
		Instant start = Instant.now();
		
		List<String> filesToDownload = wconf().get("files.input", true);
		String localDownloadPath = wconf().get("files.local_download_path");
		String s3Bucket = wconf().get("aws.s3.bucket");
		String s3KeyPrefix = wconf().get("aws.s3.object_key_prefix");
		
		LOGGER.info("About to process {} file{}", filesToDownload.size(), filesToDownload.size() == 1? "" : "s");
		
		filesToDownload.stream()
			.parallel()
			.map(file -> {
				LOGGER.debug("About to process file from list {}", file);
				try {
					return new URL(file);
				} catch (MalformedURLException e) {
					LOGGER.error("Could not create URL from {}", file, e);
					throw new IllegalArgumentException("Could not create URL", e);
				}
			})
			.forEach(fileUrl -> {
				Instant startDownload = Instant.now();
				Path localDownloadFilePath = Paths.get(localDownloadPath, fileUrl.getFile());
				LOGGER.debug("About to download file {} -> {}", fileUrl, localDownloadFilePath);
				try {
					FileUtils.copyURLToFile(fileUrl, localDownloadFilePath.toFile());
					LOGGER.debug("Download completed for {}: {}", fileUrl, Duration.between(startDownload, Instant.now()));
					
					Instant startGunzip = Instant.now();					
					LOGGER.debug("About to gunzip {}", localDownloadFilePath);
					Path gunzippedFilePath = gunzipFile(localDownloadFilePath);
					LOGGER.debug("Gunzipping completed for {}: {}", localDownloadFilePath, Duration.between(startGunzip, Instant.now()));
					
					Instant startUpload = Instant.now();
					LOGGER.debug("About to upload {} -> s3://{}/{}", gunzippedFilePath, s3Bucket, s3KeyPrefix);
					uploadLocalFileToS3(gunzippedFilePath, s3Bucket, s3KeyPrefix);
					LOGGER.debug("Upload completed for s3://{}/{}: {}", s3Bucket, s3KeyPrefix, Duration.between(startUpload, Instant.now()));
					
				} catch (IOException e) {
					LOGGER.error("Could not process file from URL {}", fileUrl, e);
					throw new IllegalArgumentException("Could not copy file from URL", e);
				}
			});		
		
		LOGGER.info("Processing took {}", Duration.between(start, Instant.now()));
	}
	
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
	
	private static void uploadLocalFileToS3(Path inputFile, String s3Bucket, String s3ObjectKeypREFIX) {
		String awsProfile = wconf().get("aws.profile");
		String awsRegion = wconf().get("aws.region");
		String s3UploadingNumThreads = wconf().get("aws.s3.num_threads");
		
		Path objectKey = Paths.get(s3ObjectKeypREFIX, inputFile.getFileName().toString());
		
		final AmazonS3 client = AmazonS3ClientBuilder
									.standard()
									.withCredentials(new ProfileCredentialsProvider(awsProfile))
									.withRegion(awsRegion)
									.build();
				
		
		TransferManager transferManager = TransferManagerBuilder.standard()
											.withExecutorFactory(() -> Executors.newFixedThreadPool(Integer.parseInt(s3UploadingNumThreads)))
											.withMinimumUploadPartSize(Long.parseLong(wconf().get("aws.s3.min_upload_part_size")))
											.withS3Client(client)
											.build();
		
		/* previous approach can be simplified using the default values */
//		TransferManager transferManager = TransferManagerBuilder.standard()
//											.withS3Client(client)
//											.build();
		
		Upload upload = transferManager.upload(s3Bucket, objectKey.toString(), inputFile.toFile());
		
		try {
			upload.waitForCompletion();
			LOGGER.debug("Completed s3://{}/{}", s3Bucket, objectKey.toString());
		} catch (AmazonClientException | InterruptedException e) {
			LOGGER.error("Could not complete upload for {} into s3://{}/{}. You might need to abort the multipart upload using the CLI.", inputFile, s3Bucket, objectKey, e);			
			throw new IllegalStateException("Could not complete upload", e);			
		}	
	}
}
