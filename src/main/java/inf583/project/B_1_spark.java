package inf583.project;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class B_1_spark {

	final static String input1 = "data/graph/edgelist.txt";
	final static String input2 = "data/graph/idslabels.txt";

	public static void main(String[] args) {
		long timeA = System.currentTimeMillis();
		B_1_spark.question1_spark(3, false);
		long timeB = System.currentTimeMillis();
		System.out.println("Time question1 spark: " + (timeB - timeA));
	}
	
	public static JavaPairRDD<String, Double> question1_spark(int T, boolean verbose) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("eigenvector_centrality");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		JavaRDD<String> input = sc.textFile(input1);
		JavaRDD<String> input_id = sc.textFile(input2);

		JavaPairRDD<String, Double> r = input_id
				.mapToPair(line -> new Tuple2<>(String.valueOf(line.split(" ")[0]), (double) 1.0)); // creating r0

		final Double norm = r.map(x -> x._2).reduce((a, b) -> a + b);

		r = r.mapValues(x -> x / norm);

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

		sc.close();
		return r;
	}
}
