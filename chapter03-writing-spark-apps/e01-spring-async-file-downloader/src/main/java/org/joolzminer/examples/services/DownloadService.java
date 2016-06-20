package org.joolzminer.examples.services;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;







import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.AsyncClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;
import org.joolzminer.examples.exceptions.ExitException;;


@Service
public class DownloadService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DownloadService.class);

	private static final int BUFFER_SIZE_IN_BYTES = 1024;
	
	@Value("${application.outputPath:src/main/resources}")
	private String pathPrefix;
	
	@Autowired
	private AsyncClientHttpRequestFactory asyncClientHttpRequestFactory;
	
	
	
	public void download(List<URI> uris) {

		try {
			Files.createDirectories(Paths.get(pathPrefix));
		} catch (IOException e) {
			LOGGER.error("Error creating output directory {}", pathPrefix, e);
			throw new IllegalStateException("Error creating output directory");
		}
		
		Stream<Pair<URI, ListenableFuture<ClientHttpResponse>>> asyncRequestPairs =
				uris.stream()
					.map(uri -> {
						ListenableFuture<ClientHttpResponse> asyncRequest;
						try {
							asyncRequest = asyncClientHttpRequestFactory.createAsyncRequest(uri, HttpMethod.GET).executeAsync();
						} catch (IOException e) {
							throw new ExitException(e);
						}
						return Pair.of(uri, asyncRequest);
					});
			

		List<ListenableFuture<ClientHttpResponse>> responses = new ArrayList<>(uris.size());
		asyncRequestPairs.forEach(requestPair -> {
			String[] uriPathComponents = requestPair.getLeft().getPath().split("/");
			String filename = uriPathComponents[uriPathComponents.length - 1];
			
			requestPair.getRight().addCallback(getOnSuccessCallback(filename), onFailure);
			responses.add(requestPair.getRight());
		});

		LOGGER.debug("Waiting for completion: {} task(s)", responses.size());
		responses.stream()
			.forEach(response -> {
				try {
					response.get();
					LOGGER.debug("Response completed");
				} catch (Exception e) {
					LOGGER.error("Error waiting for response to complete", e);
					throw new ExitException(e);
				}
			});
	}
	
	

	private SuccessCallback<ClientHttpResponse> getOnSuccessCallback(String filename) {
		LOGGER.debug("Registering processing callback for `{}`", filename);
		
		SuccessCallback<ClientHttpResponse> onSuccess = (clientHttpResponse) -> processDownloadedFile(clientHttpResponse, filename);
		
		return onSuccess;
	}
	
	private FailureCallback onFailure = e -> {
		LOGGER.debug("Failure callback has been activated", e);
		throw new ExitException(e);
	};
	
	private void processDownloadedFile(ClientHttpResponse response, String filename) {
		LOGGER.debug("About to process: `{}`", filename);
		
		String uncompressedFilename = filename.substring(0, FilenameUtils.indexOfExtension(filename));

		
		byte[] buffer = new byte[BUFFER_SIZE_IN_BYTES];		
		try (GZIPInputStream gzIn = new GZIPInputStream(response.getBody());				
				OutputStream out = Files.newOutputStream(Paths.get(pathPrefix, uncompressedFilename), StandardOpenOption.CREATE)) {
			int len;
			while ((len = gzIn.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}							
		} catch (IOException e) {
			LOGGER.debug("Problem found while gunzipping file: {} -> {}", filename, uncompressedFilename);
			throw new ExitException(e);
		}
		LOGGER.debug("Successfully processed: `{}` => `{}`", filename, uncompressedFilename);
	}
	
	
}
