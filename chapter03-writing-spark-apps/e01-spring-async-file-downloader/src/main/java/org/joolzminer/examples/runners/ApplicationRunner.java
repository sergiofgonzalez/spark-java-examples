package org.joolzminer.examples.runners;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.joolzminer.examples.exceptions.ExitException;
import org.joolzminer.examples.services.DownloadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import static java.util.stream.Collectors.*;

@ConfigurationProperties(prefix = "application")
@Component
public class ApplicationRunner implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationRunner.class);
	
	@Autowired
	private DownloadService downloadService;
	
	@Autowired
	private ConfigurableApplicationContext ctx;
	
	private List<String> files = new ArrayList<String>();
	
	@Override
	public void run(String... args) throws Exception {
		LOGGER.debug("About to process the following files: {}", files);
		downloadService.download(files.stream()
									.map(file -> {
										URI fileAsUri;
										try {
											fileAsUri = new URI(file);
										} catch (URISyntaxException e) {
											LOGGER.debug("Invalid syntax for file: {}", file, e);
											throw new ExitException(e);
										}
										return fileAsUri;
									}).collect(toList()));
		
		if (ctx.isActive()) {
			LOGGER.debug("Closing context");
			ctx.close();			
		}

	}
	
	public List<String> getFiles() {
		return this.files;
	}

}
