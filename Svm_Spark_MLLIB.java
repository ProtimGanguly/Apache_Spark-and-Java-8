package spark_test;

import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
//import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;


/**
 * Example for SVMWithSGD.
 */
public class Svm_Model 
{
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) 
	{
			
			// To set Debug OFF for Apache Spark Java
	    	Logger.getLogger("org").setLevel(Level.OFF);
	    	Logger.getLogger("akka").setLevel(Level.OFF);
			System.setProperty("hadoop.home.dir", "C:/winutils");
			SparkConf sparkConf = new SparkConf().setAppName("Svm_Model").setMaster("local[4]").set("spark.executor.memory",
					"1g");
			// Creating the Java Spark Context
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
		    try
		    {
		    	// Initializing the dataset given
		    	File f = new File("src/spark_test/imdb_labelled.txt");
		    	
		    	File pfile = new File("src/spark_test/Postive.txt");
		    	FileWriter pwriter = new FileWriter(pfile);
			    //Create the file which will contain positive sentiments
				if (pfile.createNewFile())
				{System.out.println("File is created!");}
		    	File nfile = new File("src/spark_test/Negative.txt");
		    	FileWriter nwriter = new FileWriter(nfile);
		    	////Create the file which will contain negative sentiments
				if (nfile.createNewFile())
				{System.out.println("File is created!");}
				
			    Scanner scanner = new Scanner(f);
			    //Reading through our file dataset imdb_labelled.txt
			    while(scanner.hasNextLine()){
			    	// Splitting each line based on tab
			        String[] tokens = scanner.nextLine().split("\\t");
			        // Check if the label is 1 then write the positive reviews in a file
			        if (tokens[1].equals("1"))
			        {
			        	pwriter.write(tokens[0]);
			        	pwriter.write("\n"); 
			        }
			        // Check if the label is 0 then write the negative reviews in a file
			        else if (tokens[1].equals("0"))
			        {
			        		nwriter.write(tokens[0]);
			        		nwriter.write("\n");
			        }
			    }
			    pwriter.close();
			    nwriter.close();
		    }catch (Exception e) {         
	            e.printStackTrace();
	        }
		    // RDD of lines of(Positive Review) from Positive File
		    JavaRDD<String> posreview= sc.textFile("src/spark_test/Postive.txt");
		    // RDD of lines of(Positive Review) from Positive File
		    JavaRDD<String> negreview= sc.textFile("src/spark_test/Negative.txt");
		    // Create a HashingTFinstance to map sentiment text to vectors of 10000 features:
		    final HashingTF tf= new HashingTF(10000);
		    // Create LabeledPointdatasets for positive and negative examples
		    JavaRDD<LabeledPoint> positiveExamples= posreview.map(i -> 
		    new LabeledPoint(1/*pos*/, tf.transform(Arrays.asList(i.split(" ")))));
		    JavaRDD<LabeledPoint> negativeExamples= negreview.map(i -> 
		    new LabeledPoint(0/*neg*/, tf.transform(Arrays.asList(i.split(" ")))));
		    JavaRDD<LabeledPoint> data= positiveExamples.union(negativeExamples);
		    // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
		    training.cache();
		    JavaRDD<LabeledPoint> test = data.subtract(training);
		
		    // Run training algorithm to build the model.
		    int numIterations = 100;
		    SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);
		    // Compute raw scores on the test set.
		    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(p ->
		      new Tuple2<>(model.predict(p.features()), p.label()));
		    //Printing the predicted and Actual Label the classifier computed for the Test Set.
		    for (Tuple2<?, ?> tuple : scoreAndLabels.collect())
				System.out.println("The predicted Labels(sentiments) " + tuple._1() + " and the actual Label (Sentiment) of the movie Review " + tuple._2());
		    
		    // Get evaluation metrics.
		    BinaryClassificationMetrics metrics =
		      new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
		    double auROC = metrics.areaUnderROC();
		
		    System.out.println("Area under ROC = " + auROC);
		    sc.stop();
  }
}