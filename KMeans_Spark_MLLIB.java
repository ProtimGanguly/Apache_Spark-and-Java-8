import scala.Tuple2;
import scala.collection.SortedMap;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
//import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.function.Function;

/**
 * Example for SVMWithSGD.
 */
public class KMeans_Spark 
{
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) 
	{
			
			// To set Debug OFF for Apache Spark Java
	    	Logger.getLogger("org").setLevel(Level.OFF);
	    	Logger.getLogger("akka").setLevel(Level.OFF);
			System.setProperty("hadoop.home.dir", "C:/winutils");
			SparkConf sparkConf = new SparkConf().setAppName("KMeans_Spark").setMaster("local[4]").set("spark.executor.memory",
					"1g");
			// Creating the Java Spark Context
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
		   
		 // RDD of lines of(twitter2D) text File
		    JavaRDD<String> Coordreview= sc.textFile("src/twitter2D.txt");
		   // Using transformation function map() to create RDD of only the coordinates of each line of tweet 
		    JavaRDD<Vector> parsedData = Coordreview.map(s -> {
		    	  String[] sarray = s.split(",");
		    	  double[] values = new double[2];
		    	  for (int i = 0; i < values.length; i++) {
		    		// Converting to double since every we are converting non - numerical attribute to numerical attribute
		    	    values[i] = Double.parseDouble(sarray[i]);
		    	  }
		    	  return Vectors.dense(values);
		    	});
		    // Caching the RDD to memory since we would be using this RDD for predicting results
		    parsedData.cache();
		 // Cluster the data into 4 clusters using KMeans
		    int numClusters = 4;
		    int numIterations = 40;
		    long lseed = 10;
		    // Creating an arrayList of strings to store the predictions and Tweet
		    List <String> l = new ArrayList<String>();
		    // Optional use of seed while training the classifier so that the clusters centers are fixed instead of being randomly allocated
		    //KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations, "k-means||" , lseed);
		    // Training the classifier
		    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		    // Outputting the cluster Centers
		    System.out.println("Cluster centers:");
		    for (Vector center: clusters.clusterCenters()) {
		      System.out.println(" " + center);
		    }
		    // Using the action operation collect() to perform predictions on the coordinates to respective cluster centers after training 
		    for (String v: Coordreview.collect()) {
		    	  String[] sarray = v.split(",");
		    	  double[] values = new double[2];
		    	  for (int i = 0; i < 2; i++) {
		    	    values[i] = Double.parseDouble(sarray[i]);
		    	  }
		    	  // Adding the TWEET and prediction into a list of String
		    	  l.add(clusters.predict(Vectors.dense(values)) + "TWEET" + sarray[sarray.length-1]);
			}
		    // Sorting the string in increasing order
		    Collections.sort(l);
		    // Printing in the desired output
		    for(String s : l)
		    {
		    	String sarr [] = s.split("TWEET") ;
		    	System.out.println("TWEET " + sarr[1] + "is in cluster:" + sarr[0]);
		    }
		    sc.stop();
  }
}