package inf583.project;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.lang.Math;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class B {

	final static String input1 = "data/graph/edgelist.txt";
	final static String input2 = "data/graph/idslabels.txt";

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
				for(int i = 1; i < line.length; i++) {
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
        for(int i = 0; i < r.length; i++) {
        	if(i < r.length / 2) {
        		new_r[i] = results[0][i] + results[1][i];
        	} else {
        		new_r[i] = results[2][i - r.length/2] + results[3][i - r.length/2];
        	}
        }
        return new_r;
	}
	
	public static double l2Norm(double[] r) {
		double sum = 0;
		for (double d : r) {
			sum += d * d;
		}
		return Math.sqrt(sum);
	}
	
	public static JavaPairRDD<String, Double> question1_thread(int T) {
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
		
		System.out.println("#############################");
		System.out.println("Question B.1. (Thread Version)");
		System.out.println("The value of Eigenvector centrality after " + T + " iterations is:");
		for (int i = 0; i < r.length; i++) {
			System.out.println(i + " " + r[i]);			
		}
		System.out.println("#############################");
		return null;
	}

	public static JavaPairRDD<String, Double> question1_spark(int T, boolean verbose, JavaSparkContext sc) {
		boolean shutdow = false;
		if (sc == null) {
			SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("eigenvector_centrality");
			sc = new JavaSparkContext(conf);
			sc.setLogLevel("ERROR");
			shutdow = true;
		}

		JavaRDD<String> input = sc.textFile(input1);
		JavaRDD<String> input_id = sc.textFile(input2);
		// JavaRDD<Integer> integers = input.mapT(n -> Integer.parseInt(n));

		JavaPairRDD<String, Double> r = input_id
				.mapToPair(line -> new Tuple2<>(String.valueOf(line.split(" ")[0]), (double) 1.0)); // creating r0

		final Double norm = r.map(x -> x._2).reduce((a, b) -> a + b);

		// System.out.println(norm);
		r = r.mapValues(x -> x/norm);

		// creating new A
		JavaRDD<String> A0 = input.flatMap(line -> {
			String[] arrLine = line.split(" ");
			String[] results = new String[arrLine.length - 1];
			for (int i = 1; i < arrLine.length; i++) {
				results[i - 1] = arrLine[0] + " " + arrLine[i];
			}
			return Arrays.asList(results).iterator();
		});
		// Creating pair (j,i) such that A[i][j] = 1
		JavaPairRDD<String, String> A = A0.mapToPair(line -> {
			String[] arrLine = line.split(" ");
			return new Tuple2<>(String.valueOf(arrLine[1]), String.valueOf(arrLine[0]));
		});

		for (int t = 0; t < T; t++) {
			JavaPairRDD<String, Double> r_t = A.join(r).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2))
					.reduceByKey((a, b) -> a + b);
			// r_t.collect().forEach(n->{System.out.println("r_t"+n);});
			final Double norm2 = r_t.map(x -> x._2 * x._2).reduce((a, b) -> a + b);
			r_t = r_t.mapValues(x -> x / Math.sqrt(norm2));
			r = r_t;
		}

		if (verbose) {
			System.out.println("#############################");
			System.out.println("Question B.1. (Spark Version)");
			System.out.println("The value of Eigenvector centrality after " + T + " iterations is:");
			r.collect().stream().forEach(n -> {
				System.out.println(n._1 + " " + n._2);
			});
			System.out.println("#############################");
		}
		if (shutdow)
			sc.close();
		return r;
	}

	public static void question2_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		JavaRDD<String> input = sc.textFile(input2);

		JavaPairRDD<String, String> idslabels = input.mapToPair(line -> {
			String[] arrLine = line.split(" ");
			return new Tuple2<>(arrLine[0], arrLine[1]);
		});

		JavaPairRDD<String, Double> eigenVector = question1_spark(10, false, sc);

		final Tuple2<String, Double> best_eigenCoordinate = eigenVector.map(x -> x).reduce((a, b) -> {
			if (a._2 >= b._2) {
				return a;
			} else {
				return b;
			}
		});

		JavaPairRDD<String, String> best_page = idslabels.filter(a -> a._1.equals(best_eigenCoordinate._1));

		System.out.println("#############################");
		System.out.println("Question A.2. (Spark Version)");
		best_page.collect().stream().forEach(page -> {
			System.out.println("The best page is " + page);
		});
		System.out.println("#############################"); 
		
		sc.close();
	}

	public static void main(String[] args) {
		/*long timeA = System.currentTimeMillis();
		question1_spark(3,true,null);
		long timeB = System.currentTimeMillis();
		System.out.println("Time question1 spark: " + (timeB - timeA));
		timeA = System.currentTimeMillis();
		question1_thread(3);
		timeB = System.currentTimeMillis();
		System.out.println("Time question1 thread: " + (timeB - timeA));*/
 		question2_spark();
	}
}
