package inf583.project;

import java.io.IOException;
import java.util.Arrays;
import java.lang.Math;

import org.apache.hadoop.shaded.org.eclipse.jetty.util.component.Graceful.Shutdown;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class B {
	
	final static String input1 = "data/graph/edgelist.txt";
	final static String input2 = "data/graph/idslabels.txt";
	
	public static JavaPairRDD<String,Double> question1_spark(int T, boolean verbose, JavaSparkContext sc) {
		boolean shutdow = false;
		if(sc ==null) {
			SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("eigenvector_centrality");
			sc = new JavaSparkContext(conf);
	    	sc.setLogLevel("ERROR");
	    	shutdow = true;
		}
		
    	JavaRDD<String> input = sc.textFile(input1);
    	//JavaRDD<Integer> integers = input.mapT(n -> Integer.parseInt(n));
    	
    	JavaPairRDD<String,Double> r = input.mapToPair(line -> new Tuple2<>(String.valueOf(line.split(" ")[0]),new Double(1.0))); //creating r0
    	
    	final Double norm = r.map(x->x._2*x._2).reduce((a,b)-> a+b);
    	
    	//System.out.println(norm);
    	r = r.mapValues(x -> x/norm);
    	
    	//creating new A
    	JavaRDD<String> A0 = input.flatMap(line ->{
    		String[] arrLine = line.split(" ");
    		String[] results = new String[arrLine.length -1];
    		for (int i=1; i<arrLine.length; i++) {
    			results[i-1] = arrLine[0]+" "+arrLine[i];
    		}
    		return Arrays.asList(results).iterator();
    	});
    	//Creating pair (j,i) such that A[i][j] = 1
    	JavaPairRDD<String,String> A = A0.mapToPair(line ->{
    		String [] arrLine = line.split(" ");
    		return new Tuple2<>(String.valueOf(arrLine[1]),String.valueOf(arrLine[0]));
    	});
    	
    	for(int t =1; t< T; t++) {
    		JavaPairRDD<String,Double> r_t = A.join(r).mapToPair(x->new Tuple2<>(x._2._1,x._2._2))
    				.reduceByKey((a,b) -> a+b);
    		final Double norm2 = r_t.map(x->x._2*x._2).reduce((a,b)-> a + b);
    		r_t = r_t.mapValues(x -> x/Math.sqrt(norm2));
    		r = r_t;
    		
    	}
    	
    	if (verbose) {
    		System.out.println("#############################");
        	System.out.println("Question B.1. (Spark Version)");
    		System.out.println("The value of Eigenvector centrality after "+T+" iterations is:");
    		r.collect().stream().forEach(n->{
    			System.out.println(n._1 + " "+ n._2);
    		});
    		System.out.println("#############################");	
    	}
    	if(shutdow)
    		sc.close();
		return r;
	}
	
	public static void question2_spark() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	
    	JavaRDD<String> input = sc.textFile(input2);
    	
    	JavaPairRDD<String, String> idslabels = input.mapToPair(line ->{
    		String[] arrLine = line.split(" ");
    		return new Tuple2<>(arrLine[0], arrLine[1]);
    	});
    	
    	JavaPairRDD<String,Double> eigenVector = question1_spark(4, false, sc);
    	
    	final Tuple2<String,Double>best_eigenCoordinate = eigenVector.map(x->x).reduce((a,b)->{
    		if (a._2>b._2) {
    			return a;
    		}
    		else {
    			return b;
    		}
    	});
    	
    	JavaPairRDD<String, String> best_page = idslabels.filter(a->a._1.equals(best_eigenCoordinate._1));
    	
    	System.out.println("#############################");
    	System.out.println("Question A.2. (Spark Version)");
		best_page.collect().stream().forEach(page->{
			System.out.println("The best page is "+page);
		});
		System.out.println("#############################");
		
		sc.close();
	}
	
	
	public static void main(String[] args) {
		//question1_spark(3,true,null);
		question2_spark();
	}
}
