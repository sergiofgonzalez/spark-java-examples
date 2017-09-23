package com.github.sergiofgonzalez.examples.spark.java.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.sergiofgonzalez.wconf.WaterfallConfig.*;


/**
 * Template for Spark application that streamlines Spark applications and provides a flexible configuration framework that is accessible from both the master and worker nodes. 
 * Use the SparkApplicationTemplate to streamline the execution of a Spark application that given an initialized SparkSession and JavaSparkContextn
 * and a flexible configuration properties framework that is accessible from both master and worker nodes.
 * 
 * The template defines a series of hooks that accept the actions to process, and the main method withSparkDo()
 * can be configured using a simple DSL based on the SparkApplicationConfigurationBuilder.
 * 
 * E.g.
 * 
 * 		withSparkDo(cfg -> cfg
 *						.withName("GithubDayAnalysis")
 *						.withPropertiesFile("config/spark-job.conf")
 *						.withExtraConfigVars(args)
 *						.doing(githubAnalysisApp)
 *						.afterSparkInitializedDo(awsAwareInitializationHook)
 *						.submit());	
 * 
 * 
 * @author sergio.f.gonzalez
 *
 */
public class SparkApplicationTemplate {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkApplicationTemplate.class);
	
	/**
	 * Call this function to initiate a Spark application according to the parameterization given as argument.
	 * 
	 * @param cfgFn A function that receives an SparkApplicationConfigurationBuilder and returns a SparkApplicationConfiguration
	 */
	public static void withSparkDo(Function<SparkApplicationConfigurationBuilder, SparkApplicationConfiguration> cfgFn) {
		Instant start = Instant.now();
		
		SparkApplicationConfiguration cfg = cfgFn.apply(new SparkApplicationConfigurationBuilder());
		LOGGER.info("Received request to initialize Spark application through SparkApplicationTemplate for: {}", cfg.applicationName);		
		
		Instant startSparkSessionCreation = Instant.now();
		SparkSession spark = getConfigBasedSparkSession(cfg.applicationName, cfg.applicationPropertiesFile, cfg.extraConfigVars);
		LOGGER.debug("{}: Config-based Spark Session Initialization took {} to complete", cfg.applicationName, Duration.between(startSparkSessionCreation, Instant.now()));
		
		try (JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext())) {
			LOGGER.debug("{}: About to process configured actions", cfg.applicationName);
			Instant startInitializationHook = Instant.now();
			if (cfg.afterSparkInitialized != null) {
				LOGGER.debug("{}: About to execute initialization hook", cfg.applicationName);
				cfg.afterSparkInitialized.accept(spark, sparkContext);
				LOGGER.debug("{}: initialization hook took {} to complete", cfg.applicationName, Duration.between(startInitializationHook, Instant.now()));
			}
			
			Instant startApp = Instant.now();
			LOGGER.debug("{}: About to execute the Spark application", cfg.applicationName);
			cfg.sparkApplication.accept(spark, sparkContext);
			LOGGER.debug("{}: Spark app took {} to complete", cfg.applicationName, Duration.between(startApp, Instant.now()));
			
			if (cfg.afterProcessingCompleted != null) {
				Instant startCleanup = Instant.now();
				LOGGER.debug("{}: About to execute clean-up hook", cfg.applicationName);
				cfg.afterProcessingCompleted.accept(spark, sparkContext);
				LOGGER.debug("{}: Cleanup hook took {} to complete", cfg.applicationName, Duration.between(startCleanup, Instant.now()));
			}
		} catch (Exception e) {
			LOGGER.error("{}: Spark Application execution has failed", cfg.applicationName, e);
			throw new IllegalStateException("Spark Application could not be successully completed", e);
		} finally {
			spark.stop();
			Duration duration = Duration.between(start, Instant.now());
			LOGGER.info("{}: Spark application took {}", cfg.applicationName, duration);
		}		
	}
	
	private static SparkSession getConfigBasedSparkSession(String applicationName, String applicationPropertiesFile, Map<String,String> extraConfigVars) {		
		LOGGER.debug("Initializing config based Spark Session for {}", applicationName);
		
		/* pre-initialize application level properties for non-clustered runs */
		String activeApplicationPropertiesFile = applicationPropertiesFile;
		if (System.getenv("wconf_app_properties") == null && System.getProperty("wconf_app_properties") == null && !extraConfigVars.containsKey("wconf_app_properties")) {
			LOGGER.debug("setting system property {}={}. Value received in method invocation has not been overridden", "wconf_app_properties", activeApplicationPropertiesFile);
			System.setProperty("wconf_app_properties", activeApplicationPropertiesFile);
		} else {
			if (System.getProperty("wconf_app_properties") != null) {
				activeApplicationPropertiesFile = System.getProperty("wconf_app_properties");
			} else if (System.getenv("wconf_app_properties") != null) {
				activeApplicationPropertiesFile = System.getenv("wconf_app_properties");				
			} else if (extraConfigVars.containsKey("wconf_app_properties")) {
				activeApplicationPropertiesFile = extraConfigVars.get("wconf_app_properties");
			}
			LOGGER.debug("wconf_app_properties value overridden by {}", activeApplicationPropertiesFile);
		}

		SparkConf sparkConf = new SparkConf()
				.setAppName(applicationName)
				.set("wconf_app_properties", activeApplicationPropertiesFile)
				.setExecutorEnv("wconf_app_properties", activeApplicationPropertiesFile);
		
		/* set command-line vars as Java system properties, add them to SparkConf later */
		LOGGER.debug("Adding command line args to Java System Properties and Spark Env. Num entries to process: {}", extraConfigVars.size());
		for (Entry<String, String> entry : extraConfigVars.entrySet()) {
			LOGGER.debug("Adding {} as Java system property and Spark Env", entry.getKey());
			System.setProperty(entry.getKey(), entry.getValue());
			sparkConf = sparkConf
					.set(entry.getKey(), entry.getValue())
					.setExecutorEnv(entry.getKey(), entry.getValue());			
		}
		
		/* This is the first call to wconf()... from now on config properties will be frozen */
		String sparkMaster = wconf().get("spark.master");
		if (!sparkMaster.isEmpty()) {
			LOGGER.info("Spark master for this application: {}", sparkMaster);
			sparkConf = sparkConf.setMaster(sparkMaster);
		} else {
			LOGGER.info("Spark master taken from cluster configuration/command line");
		}
		
		/* Once sparkConf is passed to Spark it can no longer be modified */
		SparkSession spark = SparkSession
				.builder()
				.config(sparkConf)
				.getOrCreate();
		
		LOGGER.debug("Spark Session has been successfully initialized and is ready to be handed over to {}", applicationName);
		
		return spark;
	}
		
	/**
	 * Static nested class used to provide a simple DSL to configure the template.
	 * 
	 * @author sergio.f.gonzalez
	 *
	 */
	public static final class SparkApplicationConfigurationBuilder {
		private String applicationName;
		private String applicationPropertiesFile;
		private BiConsumer<SparkSession, JavaSparkContext> afterSparkInitialized = null;
		private BiConsumer<SparkSession, JavaSparkContext> sparkApplication = null;
		private BiConsumer<SparkSession, JavaSparkContext> afterProcessingCompleted = null;
		private Map<String,String> extraConfigVars = new HashMap<>();
		
		private SparkApplicationConfigurationBuilder() {			
		}
		
		/**
		 * Use this method provide the Spark application name. The Spark application name is a required
		 * parameter.
		 * 
		 * @param name the Spark Application Name.
		 * @return The current SparkApplicationConfigurationBuilder for chaining methods
		 */
		public SparkApplicationConfigurationBuilder withName(String name) {
			this.applicationName = name;
			return this;
		}
		
		/**
		 * Use this method provide the path of the application properties files to be used. This value is
		 * optional.
		 * Note that the value passed can be overridden by other sources of configuration properties (e.g. command-line args).
		 * 
		 * @param applicationPropertiesFile the file and location of the application-level properties file (if any).
		 * @return The current SparkApplicationConfigurationBuilder for chaining methods
		 */		
		public SparkApplicationConfigurationBuilder withPropertiesFile(String applicationPropertiesFile) {
			this.applicationPropertiesFile = applicationPropertiesFile;
			return this;
		}
		
		/**
		 * Use this method to provide the processing actions to be executed by Spark. The configured function will receive
		 * an already configured SparkSession and JavaSparkContext and will be free to safely call wconf() to obtain
		 * configuration properties both from the master and executors.
		 * 
		 * This parameter is required.
		 * 
		 * @param sparkApplication A BiConsumer function with the actions to be carried out by Spark.
		 * @return The current SparkApplicationConfigurationBuilder for chaining methods
		 */
		public SparkApplicationConfigurationBuilder doing(BiConsumer<SparkSession, JavaSparkContext> sparkApplication) {
			this.sparkApplication = sparkApplication;
			return this;
		}

		/**
		 * Use this method to provide a actions to be carried out after SparkSession and wconf has been safely initialized, but
		 * before any other Spark processing takes place.
		 * 
		 *  This can be useful to configure additional execution aspects such as the file system to use or token keys for
		 *  authenticated services.
		 *  
		 * @param afterSparkInitialized A Biconsumer function with the actions to be carried out.
		 * @return The current SparkApplicationConfigurationBuilder for chaining methods
		 */
		public SparkApplicationConfigurationBuilder afterSparkInitializedDo(BiConsumer<SparkSession, JavaSparkContext> afterSparkInitialized) {
			this.afterSparkInitialized = afterSparkInitialized;
			return this;
		}
		
		/**
		 * Use this method to provide a actions to be carried out after the Spark application has been successfully completed, but before
		 * the Spark Session has been stopped.
		 * 
		 * Note that these actions will not be executed in case of error.			
		 *  
		 * @param afterProcessingCompleted A Biconsumer function with the actions to be carried out.
		 * @return The current SparkApplicationConfigurationBuilder for chaining methods
		 */
		public SparkApplicationConfigurationBuilder afterProcessingCompletedDo(BiConsumer<SparkSession, JavaSparkContext> afterProcessingCompleted) {
			this.afterProcessingCompleted = afterProcessingCompleted;
			return this;
		}

		
		/**
		 * Use this method to provide additional configuration properties to wconf that will take precedence over any
		 * other configuration properties.
		 * 
		 * You will typically want to call this function with whatever command line arguments you have received from the command-line,
		 * but the function also allows you to actuate on the parameters received to perform additional changes, or to fabricate
		 * programmatically additional args.
		 * 
		 * Only parameters with the syntax -X property=value or --extra-config property=value will be used. Any other arguments
		 * will be silently ignored.
		 * 
		 * @param args the extra configuration variables for wconf.
		 * @return
		 */
		public SparkApplicationConfigurationBuilder withExtraConfigVars(String... args) {
			CommandLineParser parser = new BasicParser();
			
			@SuppressWarnings("static-access")
			Option property = OptionBuilder
									.withArgName("property=value")
									.hasArgs(2)
									.withValueSeparator()
									.withDescription("extra options for config properties")
									.hasOptionalArgs()
									.withLongOpt("extra-config")
									.create("X");
			

			Options options = new Options();
			options.addOption(property);
			try {
				CommandLine cmd = parser.parse(options, args);
				Properties parsedArgs = cmd.getOptionProperties("X");
				parsedArgs.keySet().stream().forEach(key -> {
					this.extraConfigVars.put(key.toString(), parsedArgs.get(key).toString());
				});								
			} catch (ParseException e) {
				LOGGER.error("Could not parse extra config vars passed in the command line", e);
				throw new IllegalArgumentException("Could not parse extra config vars passed in the command line", e);				
			}
			
			
			extraConfigVars.entrySet().stream().forEach(entry -> this.extraConfigVars.put(entry.getKey(), entry.getValue()));
			return this;
		}
		
		/**
		 * The builder method. It creates a configuration object that will be subsequently used to tailor the SparkSession
		 * and execute the given actions.
		 * 
		 * @return A SparkApplicationConfiguration object
		 */
		public SparkApplicationConfiguration submit() {
			return new SparkApplicationConfiguration(applicationName, applicationPropertiesFile, afterSparkInitialized, sparkApplication, afterProcessingCompleted, extraConfigVars);
		}
		

	}
	
	/**
	 * Static nested class that models the parameters used to tailor the SparkSession that will be used
	 * to execute the Spark application as configured by the template.
	 * 
	 * @author sergio.f.gonzalez
	 *
	 */
	public static class SparkApplicationConfiguration {
		private String applicationName;
		private String applicationPropertiesFile;
		private BiConsumer<SparkSession, JavaSparkContext> afterSparkInitialized = null;
		private BiConsumer<SparkSession, JavaSparkContext> sparkApplication = null;
		private BiConsumer<SparkSession, JavaSparkContext> afterProcessingCompleted = null;
		private Map<String,String> extraConfigVars = null;

		private SparkApplicationConfiguration(String applicationName, String applicationPropertiesFile,	BiConsumer<SparkSession, JavaSparkContext> afterSparkInitialized, BiConsumer<SparkSession, JavaSparkContext> sparkApplication, BiConsumer<SparkSession, JavaSparkContext> afterProcessingCompleted, Map<String,String> extraConfigVars) {
			validateRequiredParams();
			this.applicationName = applicationName;
			this.applicationPropertiesFile = applicationPropertiesFile;
			this.afterSparkInitialized = afterSparkInitialized;
			this.sparkApplication = sparkApplication;
			this.afterProcessingCompleted = afterProcessingCompleted;
			this.extraConfigVars = Collections.unmodifiableMap(extraConfigVars);
		}
		
		private void validateRequiredParams() {
			List<String> errors = new ArrayList<>();
			if (applicationName == null || applicationName.isEmpty()) {
				errors.add("ApplicationName is a required parameter");
			}
			
			if (sparkApplication == null) {
				errors.add("Cannot execute SparkApplicationTemplate without nothing configured to process");
			}
		}
	}
}
