package org.joolzminer.examples.spark.java;

import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SparseMatrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class AppRunner {
	
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	
	public static void main(String[] args) throws IOException {
		
		/* Custom dense matrix */
		Matrix denseMatrix = Matrices.dense(2, 3, new double[] {5., 0., 0., 3., 1., 4.});
		System.out.println(denseMatrix.toString());
		printSeparator();
		
		/* 4x4 Identity Matrix */
		Matrix identityMatrix = Matrices.eye(4);
		System.out.println(identityMatrix);
		printSeparator();
		
		/* 4x2 Ones matrix */
		Matrix ones4x2 = Matrices.ones(4, 2);
		System.out.println(ones4x2);
		printSeparator();
		
		/* 2x4 Zeros matrix */
		Matrix zeros2x4 = Matrices.zeros(2,  4);
		System.out.println(zeros2x4);
		printSeparator();
		
		/* Diagonal matrix */
		Vector vDiag = Vectors.dense(1., 2., 3., 4., 5.);
		Matrix diagMatrix = Matrices.diag(vDiag);
		System.out.println(diagMatrix);
		printSeparator();
		
		/* Random matrix */
		Matrix randomUniformMatrix = Matrices.rand(10, 10, new Random());
		System.out.println(randomUniformMatrix);
		System.out.println();
		Matrix randomGaussianMatrix = Matrices.randn(10, 10, new Random());
		System.out.println(randomGaussianMatrix);
		printSeparator();
		
		
		/* Sparse matrix using CSC format */
		Matrix sparseMatrix = Matrices.sparse(2, 3, new int[] {0, 1, 2, 4}, new int[] {0, 1, 0, 1}, new double[] {5., 3., 1., 4. });
		System.out.println(sparseMatrix);
		printSeparator();

		// Conversion to dense
		System.out.println(((SparseMatrix)sparseMatrix).toDense());
		printSeparator();
		
		// Conversion of dense to sparse
		System.out.println(((DenseMatrix)denseMatrix).toSparse());
		printSeparator();
		
		
		/* custom iteration */
		prettyPrintLocalMatrix(denseMatrix);
		printSeparator();
	}
	
	private static void printSeparator( ) {
		System.out.println("======================================================================");
	}
	
	private static void prettyPrintLocalMatrix(Matrix matrix) {
		double[] matrixAsArray = matrix.toArray();
		
		/* 
		 * Note that i get the elements from left to right, top to bottom, first by col, then by row, 
		 * so the first thing to do is transpose (use j to select the row, i to select the row)
		 * 
		 */
		
		for (int i = 0; i < matrix.numRows(); i++) {
			for (int j = 0; j < matrix.numCols(); j++) {
				System.out.print(String.format("%9.3f", matrixAsArray[j * matrix.numRows() + i]));
			}
			System.out.println();
		}
		
	}
}
