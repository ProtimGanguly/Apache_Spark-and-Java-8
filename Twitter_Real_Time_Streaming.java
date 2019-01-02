
import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import scala.languageFeature.implicitConversions;
import twitter4j.*;
public class Twitter_Stream{
	
	public static Long globalCount = 0L, x = 0L;
	
	public static void main(String[] args) throws Exception{
	// To set Debug OFF for Apache Spark Java
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
		
	// Set the system properties so that Twitter4j library used by Twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "PjYDG0tQNdB1iBIns9Hb7I5nN");
    System.setProperty("twitter4j.oauth.consumerSecret", "Q1bJHeBW5etkXATQNfr7NBxi5D4YgaCmNtBFGkiGpGUH5jfwK4");
    System.setProperty("twitter4j.oauth.accessToken", "1065995051757723649-0PkafUmY4eQJy43L7VIM3UbgYDnKcW");
    System.setProperty("twitter4j.oauth.accessTokenSecret", "I3nB9r5p9rO2ARFgbQcS6OzkP8aii0Ar3DI3eysBzuWPp");

    
    SparkConf sparkConf = new SparkConf().setAppName("Twitter_Streaming");
    
    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]");
    }
    // To create a StreamingContext object that returns a stream every 1 second
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
    jssc.checkpoint("/checkpoint/Sample");
    JavaDStream<Status> stream = TwitterUtils.createStream(jssc);
    
    //============================PART 1======================================================
    // The above DStream is continuous stream of RDD's from which we can extract the TWEET and print
    JavaDStream<String> statuses = stream.map(
    	      status -> status.getText());
    // Printing the Tweet
    statuses.print();
    //========================================================================================
    //===========================PART 2======================================================
    // Fetching the count of Words from each Tweet
    JavaDStream<Integer> wordsInTweet = statuses.map(s -> Arrays.asList(s.split(" ")).size());
    //Printing the number of words in each tweet
    wordsInTweet.print();
    // Fetching the count of Characters from each Tweet
    JavaDStream<Integer> chrInTweet = statuses.map(s -> Arrays.asList(s.split("")).size());
    //Printing the number of characters in each tweet
    chrInTweet.print();
    //Filter the words starting with Hashtags
    JavaDStream<String> Hashtags = statuses.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
    							.filter(h -> h.startsWith("#"));
    // Printing the hashtags
    Hashtags.print();
    //=======================================================================================
    //===========================Part 3====================================================
    // =====================Calculating the average========================================
    statuses.foreachRDD(rdd -> {
    	if (rdd.count() > 0)
    	{
    		JavaRDD<String> wordsinaBatch = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
    		System.out.println("Number of Words in a Batch : " + wordsinaBatch.count());
    		System.out.println("===========================================");
    		JavaRDD<String> chrinaBatch = rdd.flatMap(s -> Arrays.asList(s.split("")).iterator());
    		System.out.println("Number of characters in a Batch : " + chrinaBatch.count());
    		System.out.println("==========================================");
    		System.out.println("The Average number of words per tweet is " + (wordsinaBatch.count()/rdd.count()));
    		System.out.println("==========================================");
    		System.out.println("The Average number of characters per tweet is " + (chrinaBatch.count()/rdd.count()));
    		System.out.println("==========================================");
    	}
    });
    //==============================PART 3===============================================
    //===============Calculating the average for a sliding the window across every 30 seconds
    statuses.window(new Duration(60 * 5 * 1000),new Duration(30 * 1000)).foreachRDD(rdd -> {
    	if (rdd.count() > 0)
    	{
    		JavaRDD<String> wordsinaBatch = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
    		System.out.println("Number of Words in a Window: " + wordsinaBatch.count());
    		System.out.println("===========================================");
    		JavaRDD<String> chrinaBatch = rdd.flatMap(s -> Arrays.asList(s.split("")).iterator());
    		System.out.println("Number of characters in a Window : " + chrinaBatch.count());
    		System.out.println("==========================================");
    		System.out.println("The Average number of words per tweet is " + (wordsinaBatch.count()/rdd.count()));
    		System.out.println("==========================================");
    		System.out.println("The Average number of characters per tweet is " + (chrinaBatch.count()/rdd.count()));
    		System.out.println("==========================================");
    	}
    });
    
    //===================================PART 3 ===================================================
    // Counting the frequency of each hashtag using MapReduce and sliding the window across every 30 seconds
    JavaPairDStream<String,Integer> hastagCount = Hashtags.mapToPair(i -> new Tuple2<>(i, 1))
    											.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() 
    											  {
    										        public Integer call(Integer i1, Integer i2) { return i1 + i2; }
    										      },
    										      new Function2<Integer, Integer, Integer>() {
    										        public Integer call(Integer i1, Integer i2) { return i1 - i2; }
    										      },
    										      new Duration(60 * 5 * 1000),// for the last 5 minutes of Tweet
    										      new Duration(30 * 1000)); // Sliding the window for every 30 seconds
    
    // Since we need to sort on the count of the hashtags in descending and we have the function sortByKey so we need to
    // swap the positions as (Frequency , Hashtags)
    JavaPairDStream<Integer, String> swappedCounts = hastagCount.mapToPair(
    	     new PairFunction<Tuple2<String, Integer>, Integer, String>() {
    	       public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
    	         return in.swap();
    	       }
    	     });
    
    // Sort in descending order of the hastags
    JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(i -> i.sortByKey(false));
    
    sortedCounts.foreachRDD( i ->
    			{
    				String out = "\nTop 10 hashtags:\n";
    		         for (Tuple2<Integer, String> t: i.take(10)) {
    		           out = out + t.toString() + "\n";
    		         }
    		         System.out.println(out);
    			});
    jssc.start(); // Start the computation
    jssc.awaitTermination(); // Wait for the computation to terminate
    
	}
}
