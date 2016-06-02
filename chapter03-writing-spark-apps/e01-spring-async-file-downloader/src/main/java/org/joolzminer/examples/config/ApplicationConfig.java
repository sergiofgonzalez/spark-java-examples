package org.joolzminer.examples.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.AsyncClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsAsyncClientHttpRequestFactory;

@Configuration
public class ApplicationConfig {
	
	@Bean
	public AsyncClientHttpRequestFactory asyncClientHttpRequestFactory() {
		return new HttpComponentsAsyncClientHttpRequestFactory();
	}
}
