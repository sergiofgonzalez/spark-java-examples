package org.joolzminer.examples.spark.java.accumulables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.AccumulableParam;

/*
 * In the AccumulableParam<T,V> class:
 *   T is the type of the Accumulable  => List<Double> (to hold the sum of all the entries per metric)
 *   V is the type of the RDD elements => String[]
 */

@SuppressWarnings("serial")
public class MetricsSumAccumulableParam implements AccumulableParam<List<Double>, String[]>, Serializable {

	/*
	 * creates an initial value of the accumulable that's passed to the workers.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#zero(java.lang.Object)
	 */
	@Override
	public List<Double> zero(List<Double> accumulableInitialValue) {
		return new ArrayList<>();
	}	
	
	/*
	 * merges two accumulable values
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addInPlace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public List<Double> addInPlace(List<Double> accumulableVal1, List<Double> accumulableVal2) {
		List<Double> mergedAccumulable = new ArrayList<>();
		if (accumulableVal1.isEmpty() && accumulableVal2.isEmpty()) {
			// nothing to do
		} else if (accumulableVal1.isEmpty() && !accumulableVal2.isEmpty()) {
			mergedAccumulable.addAll(accumulableVal2);
		} else if (!accumulableVal1.isEmpty() && accumulableVal2.isEmpty()) {
			mergedAccumulable.addAll(accumulableVal1);
		} else {
			for (int i = 0; i < accumulableVal1.size(); i++) {
				mergedAccumulable.add(accumulableVal1.get(i) + accumulableVal2.get(i));
			}
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
	public List<Double> addAccumulator(List<Double> currentAccumulableValue, String[] currentElementFromRDD) {
		List<Double> resultingAccumulable = new ArrayList<>();
		/* the first metric is in the third pos */
		if (currentAccumulableValue.isEmpty()) {
			for (int i = 2; i < currentElementFromRDD.length; i++) {			
				resultingAccumulable.add(Double.valueOf(currentElementFromRDD[i]));
			}
		} else {
			for (int i = 2; i < currentElementFromRDD.length; i++) {			
				resultingAccumulable.add(currentAccumulableValue.get(i - 2) + Double.valueOf(currentElementFromRDD[i]));
			}			
		}

		return resultingAccumulable;
	}
}
