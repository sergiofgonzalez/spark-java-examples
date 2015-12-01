package org.joolzminer.examples.spark.java;

import java.util.Arrays;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import java.io.IOException;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
			Vector denseVector1 = Vectors.dense(5., 6., 7., 8. );
			Vector denseVector2 = Vectors.dense(new double[] {5., 6., 7., 8.});
			Vector sparseVector = Vectors.sparse(4, Arrays.asList(new Tuple2<>(0, 5.), new Tuple2<>(1, 6.), new Tuple2<>(2, 7.), new Tuple2<>(3, 8.)));
			
			System.out.println("denseVector1.size:" + denseVector1.size());
			System.out.println("denseVector2(2)=" + denseVector2.toDense().values()[2]);
			System.out.println("sparseVector:");
			Arrays.stream(sparseVector.toSparse().toArray()).forEach(System.out::println);
			printSeparator();
			
			prettyPrintVector(denseVector1);
	}
	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
	private static void prettyPrintVector(Vector v) {
		// i haven't been able to transform this into a lambda
		v.foreachActive(new AbstractFunction2<Object, Object, BoxedUnit>() {

			@Override
			public BoxedUnit apply(Object index, Object value) {
				System.out.println("#" + index + ": " + value);
				return BoxedUnit.UNIT;
			}
		});
	}
}
