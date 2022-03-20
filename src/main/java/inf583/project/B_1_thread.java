package inf583.project;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;

public class B_1_thread {
	
	final static String input1 = "data/graph/edgelist.txt";
	final static String input2 = "data/graph/idslabels.txt";

	public static void main(String[] args) {
		long timeA = System.currentTimeMillis();
		question1_thread(3, false);
		long timeB = System.currentTimeMillis();
		System.out.println("Time question1 thread: " + (timeB - timeA));
	}
	
	public static JavaPairRDD<String, Double> question1_thread(int T, boolean verbose) {
		ArrayList<HashSet<Integer>> edgeList = getEdgeList();
		double[] r = new double[edgeList.size()];

		for (int i = 0; i < r.length; i++) {
			r[i] = (double) 1 / r.length;
		}

		for (int i = 0; i < T; i++) {
			r = matrixMultiplicationThread(edgeList, r);
			double norm = l2Norm(r);
			for (int j = 0; j < r.length; j++) {
				r[j] = r[j] / norm;
			}
		}

		if (verbose) {
			System.out.println("#############################");
			System.out.println("Question B.1. (Thread Version)");
			System.out.println("The value of Eigenvector centrality after " + T + " iterations is:");
			for (int i = 0; i < r.length; i++) {
				System.out.println(i + " " + r[i]);
			}
			System.out.println("#############################");
		}
		return null;
	}

	public static double[] matrixMultiplicationThread(ArrayList<HashSet<Integer>> edgeList, double[] r) {
		Thread[] threads = new Thread[4];
		double[][] results = new double[4][r.length / 2 + (r.length % 2)];
		for (int i = 0; i < 4; i++) {
			threads[i] = new Thread(new BlockMatrixMultiplicationThread(edgeList, r, i, results[i]));
		}

		for (int i = 0; i < 4; i++) {
			threads[i].start();
		}

		for (int i = 0; i < 4; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		double[] new_r = new double[r.length];
		for (int i = 0; i < r.length; i++) {
			if (i < r.length / 2) {
				new_r[i] = results[0][i] + results[1][i];
			} else {
				new_r[i] = results[2][i - r.length / 2] + results[3][i - r.length / 2];
			}
		}
		return new_r;
	}

	public static ArrayList<HashSet<Integer>> getEdgeList() {
		ArrayList<HashSet<Integer>> lines = new ArrayList<HashSet<Integer>>();
		for (int i = 0; i < 64375; i++) { // 64375 the number of nodes
			lines.add(new HashSet<Integer>());
		}
		try {
			File edgelistFile = new File(input1);
			Scanner edgelistReader = new Scanner(edgelistFile);
			while (edgelistReader.hasNextLine()) {
				String[] line = edgelistReader.nextLine().split(" ");
				int id = Integer.parseInt(line[0]);
				for (int i = 1; i < line.length; i++) {
					lines.get(id).add(Integer.parseInt(line[i]));
				}
			}
			edgelistReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		return lines;
	}

	public static double l2Norm(double[] r) {
		double sum = 0;
		for (double d : r) {
			sum += d * d;
		}
		return Math.sqrt(sum);
	}

	public static class BlockMatrixMultiplicationThread implements Runnable {

		private ArrayList<HashSet<Integer>> edgeList;
		private int startI, endI, startJ, endJ;
		private double[] blockB;
		private double[] result;

		public BlockMatrixMultiplicationThread(ArrayList<HashSet<Integer>> edgeList, double[] r, int blockA,
				double[] result) {
			// blockA = 0 -> upper left block | blockA = 1 -> upper right block
			// blockA = 2 -> lower left block | blockA = 3 -> lower right block

			this.result = result;
			this.edgeList = edgeList;

			// We suppose that A is a square matrix
			int split = edgeList.size() / 2;

			// Now we construct blockA from edgeList
			if (blockA == 0) {
				startI = 0;
				endI = split;
				startJ = 0;
				endJ = split;
			} else if (blockA == 1) {
				startI = 0;
				endI = split;
				startJ = split;
				endJ = edgeList.size();
			} else if (blockA == 2) {
				startI = split;
				endI = edgeList.size();
				startJ = 0;
				endJ = split;
			} else { // blockA == 3
				startI = split;
				endI = edgeList.size();
				startJ = split;
				endJ = edgeList.size();
			}
			// Now we construct blockB from r

			this.blockB = new double[endJ - startJ];
			for (int j = startJ; j < endJ; j++) {
				blockB[j - startJ] = r[j];
			}
		}

		@Override
		public void run() {
			for (int i = startI; i < endI; i++) {
				result[i - startI] = 0;
				for (int j = startJ; j < endJ; j++) {
					if (edgeList.get(i).contains(j)) {
						result[i - startI] += blockB[j - startJ];
					}
				}
			}
		}
	}
}
