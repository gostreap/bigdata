package inf583.project;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class B_2 {
	final static String input1 = "data/graph/edgelist.txt";
	final static String input2 = "data/graph/idslabels.txt";

	public static void question2_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		JavaRDD<String> input = sc.textFile(input2);

		JavaPairRDD<String, String> idslabels = input.mapToPair(line -> {
			String[] arrLine = line.split(" ");
			return new Tuple2<>(arrLine[0], arrLine[1]);
		});

		JavaPairRDD<String, Double> eigenVector = B_1_spark.question1_spark(10, false, sc);

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
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		question2_spark();
	}
}
