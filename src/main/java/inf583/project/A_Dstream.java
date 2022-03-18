package inf583.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.io.BufferedReader;

public class A_Dstream {
	static final String inputFile = "data/integers/integers.txt";
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
		
	static Integer count  = 0;
	static Integer maximum  = Integer.MIN_VALUE;
	
	public static void question1_spark_streaming() throws IOException {
		maximum  = Integer.MIN_VALUE;
		count = 0;
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("max_integer_streaming");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
    	
    	JavaDStream<String> stream = local_stream(jssc, inputFile);  // Creating QueueStreaming from local file
    	
    	JavaDStream<Integer> Integer_stream =  stream.map(x -> Integer.parseInt(x));
    	
    	JavaDStream<Integer> max_stream =  Integer_stream.reduce( (value1, value2) -> Math.max(value1, value2)); // Calculating max for each Dstream
    	
    	// Calculating the max over all the data has been seen
        max_stream.foreachRDD(
    			x->{
    				count = count +1;
    				x.collect().stream().forEach(n-> {
						if(n > maximum) {
							maximum = n;
						}
						System.out.println("The new maximum after "+count + " windows is : "+ maximum);
						}
					);
    				
    			}
    			);
    	System.out.println("#############################");
    	System.out.println("Question A.1. (Spark streaming Version)");
		System.out.println("#############################");
		
		jssc.start();
		try {
			jssc.awaitTerminationOrTimeout(10000);
			jssc.stop();
			System.out.println("The maximum after 10 secondes of streaming is: "+ maximum);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sc.close();
	}
	
	//static Integer count  = 0;
	static Tuple2<Double,Integer> avg  = new Tuple2(0.0,0);
	public static void question2_spark_streaming() throws IOException {
		avg  = new Tuple2(0.0,0);
		count = 0;
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("avg_streaming");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
    	
    	JavaDStream<String> stream = local_stream(jssc, inputFile);  // Creating QueueStreaming from local file
    	
    	JavaDStream<Integer> Integer_stream =  stream.map(x -> Integer.parseInt(x));
    	
    	JavaDStream<Tuple2<Integer,Integer>> avg_stream =  Integer_stream.mapToPair(n -> new Tuple2<>(n,1))
    			.reduce((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
    	 // Calculating max for each Dstream
    	
    	// Calculating the max over all the data has been seen
        avg_stream.foreachRDD(
    			x->{
    				count = count +1;
    				x.collect().stream().forEach(tuple-> {
						avg = new Tuple2<>(avg._1+ tuple._1, avg._2 + tuple._2);
						System.out.println("The avg after "+count + " windows is : "+ new Double(avg._1/avg._2));
						}
					);
    				
    			}
    			);
    	System.out.println("#############################");
    	System.out.println("Question A.2. (Spark streaming Version)");
		System.out.println("#############################");
		
		jssc.start();
		try {
			jssc.awaitTerminationOrTimeout(30000);
			jssc.stop();
			System.out.println("The avg after 30 secondes of streaming is: "+ new Double(avg._1/avg._2) );
		} catch (InterruptedException e) {
			// TODO Auto-generated catch  block
			e.printStackTrace();
		}
		sc.close();
	}
	
	
	
	public static void main(String[] args) {
		try {
			question1_spark_streaming();
			question2_spark_streaming();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
