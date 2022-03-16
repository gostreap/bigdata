package inf583.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
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
    	JavaRDD<Integer> integers = input.map(n -> Integer.parseInt(n)).distinct();
    	List<Integer> set_integers = integers.collect();
    	//JavaPairRDD<Integer, Integer> tuples = integers.mapToPair(n -> new Tuple2<>(n, 1));
    	//JavaPairRDD<Integer, Integer> counts = tuples.reduceByKey((a,b) -> a+b);
    	//JavaRDD<Integer> integers_set = counts.map((a, b) -> a);
    	
    	System.out.println("#############################");
    	System.out.println("Question A.3. (Spark Version)");
		//System.out.println("The average of all the integers is " + (double) res._1 / res._2 + ".");
    	System.out.println("The set of unique integer is : ");
    	for(int i=0; i<set_integers.size(); i++) {
    		System.out.println(set_integers.get(i));
    	}
		System.out.println("#############################");
		
		sc.close();
	}
	
	public static void main(String[] args) {
		question1_spark();
		question2_spark();
		question3_spark();
	}

}
