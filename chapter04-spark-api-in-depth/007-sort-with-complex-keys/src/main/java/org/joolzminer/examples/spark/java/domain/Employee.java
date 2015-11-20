package org.joolzminer.examples.spark.java.domain;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Employee implements Serializable {
	private String firstName;
	private String lastName;
	
	protected Employee() {			
	}
	
	public Employee(String firstName, String lastName) {
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	@Override
	public String toString() {
		return "Employee [firstName=" + firstName + ", lastName=" + lastName + "]";
	}	
}
