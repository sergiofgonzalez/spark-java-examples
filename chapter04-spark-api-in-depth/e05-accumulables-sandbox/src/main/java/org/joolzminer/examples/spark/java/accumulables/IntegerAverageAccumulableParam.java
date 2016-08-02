package org.joolzminer.examples.spark.java.accumulables;

import java.io.Serializable;

import org.apache.spark.AccumulableParam;

import scala.Tuple2;

/*
 * In the AccumulableParam<T,V> class:
 *   T is the type of the Accumulable  => Tuple2<Long,Long> (_1 holds the number of elements, _2 the sum)
 *   V is the type of the RDD elements => Integer
 */

@SuppressWarnings("serial")
public class IntegerAverageAccumulableParam implements AccumulableParam<Tuple2<Long,Long>, Integer>, Serializable{

	/*
	 * creates an initial value of the accumulable that's passed to the workers.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#zero(java.lang.Object)
	 */
	@Override
	public Tuple2<Long,Long> zero(Tuple2<Long,Long> accumulableInitialValue) {
		return new Tuple2<>(0L, 0L);
	}	
	
	/*
	 * merges two accumulable values
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addInPlace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Tuple2<Long,Long> addInPlace(Tuple2<Long,Long> accumulableVal1, Tuple2<Long,Long> accumulableVal2) {
		return new Tuple2<>(accumulableVal1._1() + accumulableVal2._1(), accumulableVal1._2() + accumulableVal2._2());
	}
	
	/*
	 * adds a new value to the accumulable.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.spark.AccumulableParam#addAccumulator(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Tuple2<Long,Long> addAccumulator(Tuple2<Long,Long> currentAccumulableValue, Integer currentElementFromRDD) {
		return new Tuple2<>(currentAccumulableValue._1() + 1, currentAccumulableValue._2() + currentElementFromRDD);
	}
}
