package inf583.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;


import edu.stanford.nlp.simple.*;

public class CoreNLPSolution {
	static StanfordCoreNLP pipeline = null;
	public static StanfordCoreNLP getOrCreatePipeline ()
	{
		if (pipeline == null)
			{ 
			Properties props = new Properties();
			props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,ner,sentiment");
			pipeline = new StanfordCoreNLP(props);
			}
		return pipeline;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {


	Logger.getLogger("org").setLevel(Level.ERROR);
	Logger.getLogger("akka").setLevel(Level.ERROR);
	  	
	String consumerKey = "ADD YOUR KEY";
    String consumerSecret = "ADD YOUR KEY";
    String accessToken = "ADD YOUR KEY";
    String accessTokenSecret = "ADD YOUR KEY";
	    
	// Set the system properties so that Twitter4j library used by Twitter stream
	// can use them to generate OAuth credentials
	System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
	System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
	System.setProperty("twitter4j.oauth.accessToken", accessToken);
	System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

	SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TwitterApp");
	JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
	    
	String[] filters = {"breaking"};        
	JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
	  
    
    // Note: the sentiment is a score scale of 0 = very negative, 1 = negative, 2 = neutral, 3 = positive, and 4 = very positive.
    // the type of an entity is a string, which can be "PERSON", "ORGANIZATION", "LOCATION"
    // 1.  Compute the average sentiment of entities of type person by averaging over the sentiment of the tweets containing them.
    JavaPairDStream<String, Double> personSent = stream.flatMapToPair(s ->
    {
    	CoreDocument doc = new CoreDocument(s.getText()); 
    	getOrCreatePipeline().annotate(doc);
        double overall_sent = 0.0;
        for (CoreMap sentence : doc.annotation().get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            overall_sent +=sentiment;
        }
        overall_sent = overall_sent/doc.sentences().size();
    	ArrayList<Tuple2<String, Double>> mentions = new ArrayList<Tuple2<String, Double>>();
    	for (CoreEntityMention em : doc.entityMentions())
        	if(em.entityType().equals("PERSON")) 
        		mentions.add(new Tuple2<>(em.text(), overall_sent));
        return mentions.iterator(); 
            });; 
     
    
    
    // How can we compute the average sentiment? Note that the average is not an associative operation.
    JavaPairDStream<String, Double> personSentAvg = personSent.mapToPair(x->new Tuple2<>(x._1, new Tuple2<>(x._2, 1)))
    		.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
            .mapToPair(x -> new Tuple2<>(x._1, x._2._1 / x._2._2));
    
    personSentAvg.print();
    
    // 2.  Compute the average sentiment of entities of type location by averaging over the sentiment of the tweets containing them.
    // For this you only need to change line 115: if(em.entityType().equals("LOCATION")) 
    
    //3. Find the most frequent adjectives used in the tweets.  You can find here information on how to access the tags.  The tag of an adjective is JJ.
    JavaPairDStream<String, Integer> adjTweets= stream.flatMapToPair(s ->
    {
    	CoreDocument doc = new CoreDocument(s.getText());  
    	getOrCreatePipeline().annotate(doc);
        double overall_sent = 0.0;
        for (CoreMap sentence : doc.annotation().get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            overall_sent +=sentiment;
        }
        overall_sent = overall_sent/doc.sentences().size();
    	ArrayList<Tuple2<String, Integer>> mentions = new ArrayList<Tuple2<String, Integer>>();
    	for (CoreLabel tok : doc.tokens()) 
            if (tok.tag().contains("JJ"))
         	   mentions.add(new Tuple2<>(tok.word(), new Integer(1)));
        
 
        return mentions.iterator(); 
            });; 
    
    JavaPairDStream<String, Integer> countAdjTweets = adjTweets.reduceByKey((x, y) -> x+y);
            
    countAdjTweets.print();
    
    // 4.  Apply the operations described above on sliding windows, folowing examples from the documentation.
    JavaPairDStream<String, Integer> windowedAdjCounts = countAdjTweets.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(180), Durations.seconds(60));
    windowedAdjCounts.print();
    jssc.start();
    
	
    try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
	 }
}

