package org.joolzminer.examples.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		
		SparkConf config = new SparkConf()
				.setAppName("003-distributed-matrices")
				.setMaster("local[*]");
		
		try (JavaSparkContext sc = new JavaSparkContext(config)) {
			
			/* Create a RowMatrix */
			List<Vector> vectors = new ArrayList<>(10);
			for (int i = 0; i < 10; i++) {
				vectors.add(Vectors.dense(getVectorElements()));
			}
			
			JavaRDD<Vector> rowsRDD = sc.parallelize(vectors, 4);		
					
					
			RowMatrix rowMatrix = new RowMatrix(rowsRDD.rdd());
			System.out.println(rowMatrix.toString());
			
			/* Create an IndexedRowMatrix */
			JavaRDD<IndexedRow> indexedRows = sc.parallelize(Arrays.asList(new IndexedRow(0, vectors.get(0)), new IndexedRow(1, vectors.get(1))));
			IndexedRowMatrix indexedRowMatrix = new IndexedRowMatrix(indexedRows.rdd());
			System.out.println(indexedRowMatrix);
			
			/* convert */
			JavaRDD<IndexedRow> indexedRowsFromRowMatrix = rowMatrix.rows().toJavaRDD().zipWithIndex().map((Tuple2<Vector, Long> t) -> new IndexedRow(t._2(), t._1()));
			IndexedRowMatrix indexedRowMatrixFromRowMatrix = new IndexedRowMatrix(indexedRowsFromRowMatrix.rdd());
			System.out.println(indexedRowMatrixFromRowMatrix);
			
			/* Create a CoordinateMatrix
			 *     M = [ 5 0 1
             *           0 3 4 ] 
			 */
			JavaRDD<MatrixEntry> matrixEntries = sc.parallelize(Arrays.asList(new MatrixEntry(0, 0, 5.), new MatrixEntry(1, 1, 3.), new MatrixEntry(2, 0, 1.), new MatrixEntry(2, 1, 4.)));
			CoordinateMatrix coordMatrix = new CoordinateMatrix(matrixEntries.rdd());
			System.out.println(coordMatrix);
			printSeparator();
						
		}														
	}
	
	private static double[] getVectorElements() {
		double[] elems = new double[1_000];
		for (int i = 0; i < 1_000; i++) {
			elems[i] = (double)i;
		}
		return elems;
	}

	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
}
