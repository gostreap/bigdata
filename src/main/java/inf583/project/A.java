package inf583.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;

import scala.Tuple2;

public class A {
	
	static final String inputFile = "data/integers/integers.txt";
	
	public static void question1_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaRDD<String> input = sc.textFile(inputFile);
    	JavaRDD<Integer> integers = input.map(n -> Integer.parseInt(n));
    	
    	int max = integers.reduce( (value1, value2) -> Math.max(value1, value2));
    	
    	System.out.println("#############################");
    	System.out.println("Question A.1. (Spark Version)");
		System.out.println("The largest integer is " + max + ".");
		System.out.println("#############################");
		
		sc.close();
	}
	
	public static void question2_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaRDD<String> input = sc.textFile(inputFile);
    	JavaRDD<Integer> integers = input.map(n -> Integer.parseInt(n));
    	
    	JavaPairRDD<Integer, Integer> tuples = integers.mapToPair(n -> new Tuple2<>(n, 1));
    	Tuple2<Integer, Integer> res = tuples.reduce((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
    	
    	System.out.println("#############################");
    	System.out.println("Question A.2. (Spark Version)");
		System.out.println("The average of all the integers is " + (double) res._1 / res._2 + ".");
		System.out.println("#############################");
		
		sc.close();
	}
	
	public static void question3_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaRDD<String> input = sc.textFile(inputFile);
    	JavaRDD<Integer> integers = input.map(n -> Integer.parseInt(n));

//    	JavaRDD<Integer> integers = input.map(n -> Integer.parseInt(n)).distinct();
//    	List<Integer> set_integers = integers.collect();
    	
    	JavaPairRDD<Integer, Integer> tuples = integers.mapToPair(n -> new Tuple2<>(n, 1));
    	JavaPairRDD<Integer, Integer> counts = tuples.reduceByKey((a,b) -> 1);
    	JavaRDD<Integer> integers_set = counts.map(x -> x._1);
    	
    	System.out.println("#############################");
    	System.out.println("Question A.3. (Spark Version)");
		//System.out.println("The average of all the integers is " + (double) res._1 / res._2 + ".");
    	System.out.println("The set of unique integer is : ");
    	integers_set.foreach(x -> System.out.println(x));
		System.out.println("#############################");
		
		sc.close();
	}
	
	public static void question4_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaRDD<String> input = sc.textFile(inputFile);
    	JavaRDD<Integer> integers = input.map(n -> Integer.parseInt(n)).distinct();

//    	JavaRDD<Integer> integers_set = integers.distinct();
//    	Long count = integers_set.count();
    	
    	JavaPairRDD<Integer, Integer> tuples = integers.mapToPair(n -> new Tuple2<>(n, 1));
    	JavaPairRDD<Integer, Integer> counts = tuples.reduceByKey((a,b) -> 1);
    	JavaRDD<Integer> integers_set = counts.map(x -> x._1);
    	
    	JavaRDD<Integer> ones = integers_set.map(n -> 1);
    	Integer count = ones.reduce((a,b) -> a + b);
    	
    	System.out.println("#############################");
    	System.out.println("Question A.4. (Spark Version)");
    	System.out.println("The number of distinct element is : "+ count + ".");
		System.out.println("#############################");
		
		sc.close();
	}
	
	public static void main(String[] args) {
		question1_spark();
		question2_spark();
		question3_spark();
		question4_spark();
	}

}
