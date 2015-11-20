package org.joolzminer.examples.spark.java.accumulables;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import org.joolzminer.examples.spark.java.accumulables.utils.AccumulableFunctionHolder;

import scala.Tuple2;

/**
 * Holds the functions needed to calculate the mean of an RDD of integers as Serializable lambdas.
 *
 * @author sergio.f.gonzalez
 *
 */
@SuppressWarnings("serial")
public class AverageAccumulableFunctionHolder implements AccumulableFunctionHolder<Integer, Tuple2<Integer, Integer>>, Serializable {

	@Override
	public UnaryOperator<Tuple2<Integer, Integer>> getZeroFn() {
		return (UnaryOperator<Tuple2<Integer, Integer>> & Serializable) t-> new Tuple2<>(0, 0);
	}

	@Override
	public BinaryOperator<Tuple2<Integer, Integer>> getAddInPlaceFn() {
		return (BinaryOperator<Tuple2<Integer, Integer>> & Serializable) (v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2());
	}

	@Override
	public BiFunction<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>> getAddAccumulatorFn() {
		return (BiFunction<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>> & Serializable)(t, v) -> new Tuple2<>(t._1() + 1, t._2() + v);
	}	
}
