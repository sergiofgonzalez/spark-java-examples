package org.joolzminer.examples.spark.java.accumulables;

import java.io.Serializable;

import org.apache.spark.AccumulableParam;

/*
 * In the AccumulableParam<T,V> class:
 *   T is the type of the Accumulable  => Long (to hold the sum of all the entries)
 *   V is the type of the RDD elements => Integer
 */

@SuppressWarnings("serial")
public class IntegerSumAccumulableParam implements AccumulableParam<Long, Integer>, Serializable {

	/*
	 * creates an initial value of the accumulable that's passed to the workers.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#zero(java.lang.Object)
	 */
	@Override
	public Long zero(Long accumulableInitialValue) {
		return 0L;
	}	
	
	/*
	 * merges two accumulable values
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addInPlace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long addInPlace(Long accumulableVal1, Long accumulableVal2) {
		return accumulableVal1 + accumulableVal2;
	}
	
	/*
	 * adds a new value to the accumulable.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addAccumulator(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long addAccumulator(Long currentAccumulableValue, Integer currentElementFromRDD) {
		return currentAccumulableValue + currentElementFromRDD;
	}





}
