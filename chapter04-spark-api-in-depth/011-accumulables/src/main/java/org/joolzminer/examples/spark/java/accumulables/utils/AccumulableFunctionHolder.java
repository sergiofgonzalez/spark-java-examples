package org.joolzminer.examples.spark.java.accumulables.utils;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * Simplifies the configuration of an Accumulable using lambdas
 * 
 * @author sergio.f.gonzalez
 *
 * @param <V> the type of the RDD elements.
 * @param <T> the type of the Accumulable elements.
 */
public interface AccumulableFunctionHolder<V, T> {
	/**
	 * Must return the zero function, which creates an initial value that's passed to the
	 * workers.
	 * 
	 * @return the zero function, whose signature is T -> T, T being the type of the Accumulable elements.
	 */
	UnaryOperator<T> getZeroFn();
	
	/**
	 * Must return the addInPlace function, which merges to accumulated values.
	 *  
	 * @return the addInPlace function, whose signature is (T, T) -> T, T being the type of the Accumulable elements.
	 */
	BinaryOperator<T> getAddInPlaceFn();
	
	/**
	 * Must return the addAcumulator function, which adds a new value to the accumulable.
	 * 
	 * @return the addAcumulator function, whose signature is (T, V) -> T, T being the type of the Accumulable
	 * elements, V the type of the RDD elements
	 */
	BiFunction<T, V, T> getAddAccumulatorFn();	
}
