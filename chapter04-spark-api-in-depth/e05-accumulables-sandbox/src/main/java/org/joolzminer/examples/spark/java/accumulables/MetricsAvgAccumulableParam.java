package org.joolzminer.examples.spark.java.accumulables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.AccumulableParam;

import scala.Tuple2;

/*
 * In the AccumulableParam<T,V> class:
 *   T is the type of the Accumulable  => Tuple2<Long, List<Double>> (to hold the count and sum of all the entries per metric)
 *   V is the type of the RDD elements => String[]
 */

@SuppressWarnings("serial")
public class MetricsAvgAccumulableParam implements AccumulableParam<Tuple2<Long, List<Double>>, String[]>, Serializable {

	/*
	 * creates an initial value of the accumulable that's passed to the workers.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#zero(java.lang.Object)
	 */
	@Override
	public Tuple2<Long, List<Double>> zero(Tuple2<Long, List<Double>> accumulableInitialValue) {
		return new Tuple2<>(0L, new ArrayList<>());
	}	
	
	/*
	 * merges two accumulable values
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addInPlace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Tuple2<Long, List<Double>> addInPlace(Tuple2<Long, List<Double>> accumulableVal1, Tuple2<Long, List<Double>> accumulableVal2) {
		Tuple2<Long, List<Double>> mergedAccumulable = new Tuple2<>(accumulableVal1._1() + accumulableVal2._1(), new ArrayList<>());
		
		if (accumulableVal1._2().isEmpty() && accumulableVal2._2().isEmpty()) {
			// nothing to do
		} else if (accumulableVal1._2().isEmpty() && !accumulableVal2._2().isEmpty()) {
			mergedAccumulable._2().addAll(accumulableVal2._2());
		} else if (!accumulableVal1._2().isEmpty() && accumulableVal2._2().isEmpty()) {
			mergedAccumulable._2().addAll(accumulableVal1._2());
		} else {
			for (int i = 0; i < accumulableVal1._2().size(); i++) {
				mergedAccumulable._2().add(accumulableVal1._2().get(i) + accumulableVal2._2().get(i));
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
	public Tuple2<Long, List<Double>> addAccumulator(Tuple2<Long, List<Double>> currentAccumulableValue, String[] currentElementFromRDD) {
		Tuple2<Long, List<Double>> resultingAccumulable = new Tuple2<>(currentAccumulableValue._1() + 1, new ArrayList<>());
		
		/* the first metric is in the third pos */
		if (currentAccumulableValue._2().isEmpty()) {
			for (int i = 2; i < currentElementFromRDD.length; i++) {			
				resultingAccumulable._2().add(Double.valueOf(currentElementFromRDD[i]));
			}
		} else {
			for (int i = 2; i < currentElementFromRDD.length; i++) {			
				resultingAccumulable._2().add(currentAccumulableValue._2().get(i - 2) + Double.valueOf(currentElementFromRDD[i]));
			}			
		}

		return resultingAccumulable;
	}
}
