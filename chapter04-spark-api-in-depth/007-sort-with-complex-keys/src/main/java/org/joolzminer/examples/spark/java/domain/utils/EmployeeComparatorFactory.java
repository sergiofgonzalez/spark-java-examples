package org.joolzminer.examples.spark.java.domain.utils;

import java.io.Serializable;
import java.util.Comparator;

import org.joolzminer.examples.spark.java.domain.Employee;

/*
 * 
 * This is needed because Spark requires Serializable Comparators, which is easy to do in Java 7, but
 * requires type intersection when using lambdas.
 * 
 */

public class EmployeeComparatorFactory {
	
	public static Comparator<Employee> byFirstName() {
		return (Comparator<Employee> & Serializable) (e1, e2) -> e1.getFirstName().compareTo(e2.getFirstName());
	}
	
	public static Comparator<Employee> byLastName() {
		return (Comparator<Employee> & Serializable) (e1, e2) -> e1.getLastName().compareTo(e2.getLastName());
	}
}
