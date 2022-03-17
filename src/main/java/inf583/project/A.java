package inf583.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.rdd.RDD;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.io.BufferedReader;

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
	
	public static JavaDStream local_stream(JavaStreamingContext jssc,String input) throws IOException {
		
		JavaSparkContext jsc = new JavaSparkContext ( jssc .ssc ().sc ());
		FileReader fr = new FileReader(new File(input));
		
		BufferedReader br = new BufferedReader (fr);
		String line ;
		int count = 0;
		ArrayList <String > batch = new ArrayList < String >();
		Queue < JavaRDD < String >> rdds = new LinkedList < >();
		while (( line = br.readLine ()) != null ) {
			count +=1;
			if( count == 30)
			{
				JavaRDD <String > rdd = jsc. parallelize ( batch );
				rdds . add( rdd);
				batch = new ArrayList <String >();
				count = 0;
				
				//System.out.println(rdd.first());
			}
				batch .add( line );
		}
		JavaRDD <String > rdd = jsc. parallelize ( batch );
		rdds . add( rdd);
		JavaDStream <String > stream = jssc . queueStream (rdds , true );
		return stream;
	}
	
	public static void question1_spark_streaming() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
    	
    	JavaDStream<String> stream = local_stream(jssc, inputFile);
    	
    	JavaDStream<Integer> Integer_stream =  stream.map(x -> Integer.parseInt(x));
    	
    	JavaDStream<Integer> max =  Integer_stream.reduce( (value1, value2) -> Math.max(value1, value2));
    	
    	//Long max = 
    	
    	//concat.print();
        max.foreachRDD(
    			x->{
    				x.collect().stream().forEach(n-> System.out.println("the max for 1's windows is"+n));
    			}
    			);
    	System.out.println("#############################");
    	System.out.println("Question A.1. (Spark streaming Version)");
    	//System.out.println("The largest integer is : "+ " "+ ".");
		System.out.println("#############################");
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		sc.close();
	}
	
	public static void main(String[] args) {
		/*question1_spark();
		question2_spark();
		question3_spark();
		question4_spark();*/
		try {
			question1_spark_streaming();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
