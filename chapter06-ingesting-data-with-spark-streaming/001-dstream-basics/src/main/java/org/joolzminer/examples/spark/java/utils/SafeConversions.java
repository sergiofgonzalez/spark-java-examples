package org.joolzminer.examples.spark.java.utils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SafeConversions {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SafeConversions.class);
	
	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
	public static Integer toSafeInteger(String value) {
		Integer result = null;
		try {
			result = Integer.valueOf(value);
		} catch (NumberFormatException e) {
			// swallow and return null
			LOGGER.debug("Could not parse `{}` as an Integer. Null will be assumed.", value);			
		}
		return result;
	}
	
	public static Long toSafeLong(String value) {
		Long result = null;
		try {
			result  = Long.valueOf(value);
		} catch (NumberFormatException e) {
			// swallow and return null
			LOGGER.debug("Could not parse `{}` as a Long. Null will be assumed.", value);			
		}
		return result;
	}
	
	public static Double toSafeDouble(String value) {
		Double result = null;
		try {
			result  = Double.valueOf(value);
		} catch (NumberFormatException e) {
			// swallow and return null
			LOGGER.debug("Could not parse `{}` as a Double. Null will be assumed.", value);			
		}
		return result;
	}
	public static LocalDateTime toSafeLocalDateTime(String value) {
		LocalDateTime dt = null;
		try {
			dt = LocalDateTime.parse(value, dateTimeFormatter);
		} catch (DateTimeParseException e) {
			LOGGER.debug("Could not parse `{}` as a LocalDateTime. Attempting to use java.sql.Timestamp formatter.", value);
			// java.sql.Timestamp parse is much smarter than pattern-based DateTimeFormatter
			dt = toSafeTimestamp(value).toLocalDateTime();		
		}
		return dt;
	}
	
	public static Timestamp toSafeTimestamp(String value) {
		Timestamp ts = null;
		try {
			ts = Timestamp.valueOf(value);
		} catch (IllegalArgumentException e) {
			// swallow and return null
			LOGGER.debug("Could not parse `{}` as a Timestamp. Null will be assumed.", value);
		}
		return ts;
	}

	
}
