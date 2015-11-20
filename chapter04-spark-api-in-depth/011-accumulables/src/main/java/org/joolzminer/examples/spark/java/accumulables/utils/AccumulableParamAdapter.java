package org.joolzminer.examples.spark.java.accumulables.utils;

import java.io.Serializable;
import org.apache.spark.AccumulableParam;

@SuppressWarnings("serial")
/**
 * Simplifies the creation of an Accumulable<T, V> object by receiving a holder of the functions
 * needed by the AccumulableParam and returns them as needed.
 * 
 * @author sergio.f.gonzalez
 *
 * @param <V> The type of the RDD elements.
 * @param <T> The type of the Accumulable elements.
 */
public class AccumulableParamAdapter<V, T> implements AccumulableParam<T, V>, Serializable {
	
	private AccumulableFunctionHolder<V, T> fnHolder;
	
	public AccumulableParamAdapter(AccumulableFunctionHolder<V, T> fnHolder) {
		this.fnHolder = fnHolder;
	}
	
	@Override
	public T zero(T initialValue) {
		return fnHolder.getZeroFn().apply(initialValue);
	}

	@Override
	public T addInPlace(T value1, T value2) {
		return fnHolder.getAddInPlaceFn().apply(value1, value2);
	}
	
	@Override
	public T addAccumulator(T accumulatedValue, V accumulatorValue) {
		return fnHolder.getAddAccumulatorFn().apply(accumulatedValue, accumulatorValue);
	}
}
