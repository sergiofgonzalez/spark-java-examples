package org.joolzminer.examples.spark.java.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;


public class IntListBuilder {
	private int start;
	private int end;
	private OptionalInt step = OptionalInt.empty();
	
	private IntListBuilder() {			
	}
	
	public static IntListBuilder getInts() {
		return new IntListBuilder();
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
		for (int i = start; i <= end; i += step.orElse(1)) {
			nums.add(i);
		}
		return nums;
	}		
}
