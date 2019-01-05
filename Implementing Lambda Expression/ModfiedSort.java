package modifiedSort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class ModfiedSort {
	
	public static int arr[] = new int[10];
	private static Random r = new Random ();
	
	public static void main(String[] args) {

		// Create an unsorted array with Random numbers
		for (int i = 0; i< 10; i++) {
			arr[i] = r.nextInt(50);
		}
		System.out.println("The unsorted array" + Arrays.toString(arr));
		
		//A lambda variable that has Collection.sort() helper sort
		ImplementLamda lmd = (bucketlist) -> Collections.sort(bucketlist);
		
		// Fetching the start time of execution
		long startTime = System.currentTimeMillis();
		
		// Passing the lambda variable as a parameter
		int sortedBucket[] = bucketSort(arr, 5, lmd );
		// Fetching the end time of execution
		long endTime = System.currentTimeMillis();
		
		long duration = (endTime - startTime)/1000;
		System.out.println("The sorted array" + Arrays.toString(arr));
		System.out.println("The duration  : " + duration);

	}
	
	// Accept the lambda variable to type Interface 'ImplementLambda'
	public static int[] bucketSort(int[] numbers, int bucketCount , ImplementLamda lmd) {

		 if (numbers.length <= 1) return numbers;
		 int maxVal = numbers[0];
		 int minVal = numbers[0];

		 for (int i = 1; i < numbers.length; i++) {
		 if (numbers[i] > maxVal) maxVal = numbers[i];
		 if (numbers[i] < minVal) minVal = numbers[i];
		 }

		 double interval = ((double)(maxVal - minVal + 1)) / bucketCount; // range of bucket
		 ArrayList<Integer> buckets[] = new ArrayList[bucketCount];

		 for (int i = 0; i < bucketCount; i++) // initialize buckets (initially empty)
		 buckets[i] = new ArrayList<Integer>();

		 for (int i = 0; i < numbers.length; i++) // distribute numbers to buckets
		 buckets[(int)((numbers[i] - minVal)/interval)].add(numbers[i]);

		 int k = 0; 

		 for (int i = 0; i < buckets.length; i++) {
			 
			 	// Execute the lambda expression by calling the abstract method defined in the Interface 'ImplementLamda'
			 	lmd.doSort(buckets[i]);
			 	
			 	for (int j = 0; j < buckets[i].size(); j++) { // update array with the bucket content
			 		numbers[k] = buckets[i].get(j);
			 		k++;
			 	}
		 }

		 return numbers;
	}
}
