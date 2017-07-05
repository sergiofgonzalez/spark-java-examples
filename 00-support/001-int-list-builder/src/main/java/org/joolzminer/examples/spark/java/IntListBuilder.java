package org.joolzminer.examples.spark.java;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

/**
 * 
 * Builds a list of integers using a simple DSL approach using from(), to() and step() methods
 * 
 * Usage examples:
 * 	new IntListBuilder().from(0).to(0) => { 0 }
 *  new IntListBuilder().from(0).to(1) => { 0, 1 }
 *  new IntListBuilder().from(0).to(10).by(2) => { 0, 2, 4, 6, 8, 10 }
 *  new IntListBuilder().from(5).to(3) => { 5, 4, 3 }
 * 
 */
public class IntListBuilder {
	private int start;
	private int end;
	private OptionalInt step;
	
	public IntListBuilder() {
		step = OptionalInt.empty();
	}
	
	public IntListBuilder from(int start) {
		this.start = start;
		return this;
	}
	
	public IntListBuilder to(int end) {
		this.end = end;
		return this;
	}
	
	public IntListBuilder by(int step) {
		this.step = OptionalInt.of(step);
		return this;
	}
	
	public List<Integer> build() {
		if (!step.isPresent()) {
			step = OptionalInt.of(1);
		}
		
		List<Integer> nums = new ArrayList<>();
		if (start <= end) {
			for (int i = start; i <= end; i += step.getAsInt()) {
				nums.add(i);
			}			
		} else {
			for (int i = start; i >= end; i -= step.getAsInt()) {
				nums.add(i);
			}			
		}

		return nums;
	}
}
