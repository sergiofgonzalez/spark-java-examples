package org.joolzminer.examples.spark.java.accumulables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.AccumulableParam;

/*
 * In the AccumulableParam<T,V> class:
 *   T is the type of the Accumulable  => Map<String,List<Double>> (to hold the sum of all the entries per site)
 *   V is the type of the RDD elements => String[]
 */

@SuppressWarnings("serial")
public class MetricsSumBySiteAccumulableParam implements AccumulableParam<Map<String,List<Double>>, String[]>, Serializable {

	/*
	 * creates an initial value of the accumulable that's passed to the workers.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#zero(java.lang.Object)
	 */
	@Override
	public Map<String,List<Double>> zero(Map<String,List<Double>> accumulableInitialValue) {
		return new HashMap<>();
	}	
	
	/*
	 * merges two accumulable values
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addInPlace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Map<String,List<Double>> addInPlace(Map<String,List<Double>> accumulableVal1, Map<String,List<Double>> accumulableVal2) {
		Map<String,List<Double>> mergedAccumulable = new HashMap<>();
		
		if (accumulableVal1.entrySet().isEmpty() && accumulableVal2.entrySet().isEmpty()) {
			// nothing to do
		} else if (accumulableVal1.entrySet().isEmpty() && !accumulableVal2.entrySet().isEmpty()) {
			accumulableVal2.entrySet().stream()
				.forEach(entry -> mergedAccumulable.put(entry.getKey(), new ArrayList<>(entry.getValue())));
		} else if (!accumulableVal1.entrySet().isEmpty() && accumulableVal2.entrySet().isEmpty()) {
			accumulableVal1.entrySet().stream()
				.forEach(entry -> mergedAccumulable.put(entry.getKey(), new ArrayList<>(entry.getValue())));
		} else {
			// merge the first one
			accumulableVal1.entrySet().stream()
				.forEach(entry -> mergedAccumulable.put(entry.getKey(), new ArrayList<>(entry.getValue())));
			
			// merge the second one
			accumulableVal2.entrySet().stream()
				.forEach(entry -> {
					List<Double> mergedSumValues = new ArrayList<>();
					for (int i = 0; i < accumulableVal1.get(entry.getKey()).size(); i++) {
						mergedSumValues.add(accumulableVal1.get(entry.getKey()).get(i) + accumulableVal2.get(entry.getKey()).get(i));
					}
					mergedAccumulable.put(entry.getKey(), mergedSumValues);
				});			
		}
		return mergedAccumulable;
	}
	
	/*
	 * adds a new value to the accumulable.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addAccumulator(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Map<String,List<Double>> addAccumulator(Map<String,List<Double>> currentAccumulableValue, String[] currentElementFromRDD) {
		Map<String,List<Double>> resultingAccumulable = new HashMap<>();
		
		/* the first metric is in the third pos */
		if (currentAccumulableValue.entrySet().isEmpty()) {
			List<Double> metricValuesFromRDD = new ArrayList<Double>();
			for (int i = 2; i < currentElementFromRDD.length; i++) {			
				metricValuesFromRDD.add(Double.valueOf(currentElementFromRDD[i]));
			}
			resultingAccumulable.put(currentElementFromRDD[0], metricValuesFromRDD);
		} else {
			if (currentAccumulableValue.containsKey(currentElementFromRDD[0])) {
				currentAccumulableValue.entrySet()
				.forEach(preExistingEntry -> {
					if (preExistingEntry.getKey().equals(currentElementFromRDD[0])) {
						List<Double> metricValuesFromRDD = new ArrayList<Double>();
						for (int i = 2; i < currentElementFromRDD.length; i++) {			
							metricValuesFromRDD.add(preExistingEntry.getValue().get(i - 2) + Double.valueOf(currentElementFromRDD[i]));
						}		
						resultingAccumulable.put(preExistingEntry.getKey(), metricValuesFromRDD);						
					} else {
						resultingAccumulable.put(preExistingEntry.getKey(), preExistingEntry.getValue());						
					}

				});
			} else {
				currentAccumulableValue.entrySet()
					.forEach(preExistingEntry -> resultingAccumulable.put(preExistingEntry.getKey(), preExistingEntry.getValue()));
				List<Double> metricValuesFromRDD = new ArrayList<Double>();
				for (int i = 2; i < currentElementFromRDD.length; i++) {			
					metricValuesFromRDD.add(Double.valueOf(currentElementFromRDD[i]));
				}
				resultingAccumulable.put(currentElementFromRDD[0], metricValuesFromRDD);
			}
		}

		return resultingAccumulable;
	}
}
