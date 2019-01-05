package mapReduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeatherStation {
	
	String city;
	public static int startTime, endTime;
	public List<Measurement> measure;
	
	//############### List of temperature recordings according to the Weather Station########################
	public static List<Measurement> measure1; // Recordings for City Dublin
	public static List<Measurement> measure2; // Recordings for City Cork
	
	
	
	public static List<WeatherStation> station = new ArrayList<>();
	//Setting up constructor for the WeatherStation class
    public WeatherStation(String city, List<Measurement> measurements){
        this.city = city;
        this.measure = measurements;
        
    }
    
    //############ To get the measurements recorded cityWise###########3
    public List<Measurement> getMeasurements(){
        return measure;
    }
    public String getCity(){
        return city;
    }
    
	public static void main(String[] args) {
		// Measurements for Dublin City
		measure1 = Arrays.asList(
				 new Measurement(12 , 13.6),
				 new Measurement(13 , 14.8),
				 new Measurement(15 , 14.8),
				 new Measurement(16, 16.2),
				 new Measurement(8 , 14.4),
				 new Measurement(10 , 10.2),
				 new Measurement(18 , 9.2));
		// Measurements for Cork City
		measure2 = Arrays.asList(
				 new Measurement(14 , 13.6),
				 new Measurement(5 , 17.1),
				 new Measurement(6 , 20.8),
				 new Measurement(8 , 16.7),
				 new Measurement(17 , 10.2),
				 new Measurement(21 , 7.4));
		
		//######### Creating the object off this class with 2 cities and their Measurement##########33
		WeatherStation station1 = new WeatherStation("Dublin",measure1);
		WeatherStation station2 = new WeatherStation("Cork",measure2);
		station.add(station1);
		station.add(station2);
		//########## Calling the function for max temperature for two cities#########3
		station1.maxTemperature(8, 18);
		station2.maxTemperature(5, 17);
		
		//##############Calling count Temperature method to find the frequency using Map Reduce Approach############
		station1.countTemperature(14.4, 21.0, 2);
		
	}
	//######### Count Max Temperature Station Wise##################
	public void maxTemperature(int startTime, int endTime) {
		
		this.getMeasurements().parallelStream()
		.filter(i -> i.getTime() >= startTime)
		.filter(i -> i.getTime() <= endTime)
		.max(Comparator.comparing(i -> i.getTemperature()))
		.ifPresent(max -> System.out.println("Maximum found is " + max.getTemperature() +" for Weather Station " + this.getCity()));
	}
	
	//############### Count the Frequency of two temperatures passed in a given interval###############
	 public static void countTemperature(double t1, double t2, int r){
		 
		 //########### Creating a new List Stream by concatenating the measurements from both the Weather Station##########
		 List<Measurement> newList = Stream.concat(measure1.stream(), measure2.stream())
                 .collect(Collectors.toList());
		 
		 
		 //####### MAP PHASE ###############################		 	
		 	Map<Integer, Double> mymap1 = new HashMap<>();
		 	Map<Integer, Double> mymap2 = new HashMap<>();
		 	
		 //############## Making a key value pair with Time , Temperature for temperature t1 ######################
		 	mymap1 = newList.parallelStream()
		 	// Specifying the interval for t1
			.filter(i -> i.getTemperature() >= (t1-r))
			.filter(i -> i.getTemperature() <= (t1+r))
			.collect(Collectors.toMap(x -> x.getTime() , x -> x.getTemperature()));
	        
	        System.out.println("Map Result t1 " + mymap1);
	        
	        //#####################################
	      //############## Making a key value pair with Time , Temperature for temperature t2 ######################
	        mymap2 = newList.parallelStream()
	        		// Specifying the interval for t1
	    			.filter(i -> i.getTemperature() >= (t2-r))
	    			.filter(i -> i.getTemperature() <= (t2+r))
	    			.collect(Collectors.toMap(x -> x.getTime() , x -> x.getTemperature()));
	    	        
	    	        System.out.println("Map Result t2 " + mymap2);
	    	
	    //####### SHUFFLE PHASE ###############################
	        Map<Object, Long> finalMap1 = new LinkedHashMap<>();
	        Map<Object, Long> finalMap2 = new LinkedHashMap<>();
	        
	       //### Grouping all temperature in range t1 and counting the number of times their occurence in the specified interval############
	        finalMap1 = mymap1.entrySet().parallelStream()
	    	        //.sorted(Map.Entry.<Double, Long>comparingByValue()
	    	        .collect(Collectors.groupingBy(
	    	        	            e -> e.getValue(), Collectors.counting()));
	        
	        System.out.println("Output of Shuffle Phase for t1 is :" + finalMap1);
	        //##################################################
	      //### Grouping all temperature in range t2 and counting the number of times their occurence in the specified interval############
	        finalMap2 = mymap2.entrySet().parallelStream()
	    	        .collect(Collectors.groupingBy(
	    	        	            e -> e.getValue(), Collectors.counting()));
	        
	        System.out.println("Output of Shuffle Phase for t2 is :" + finalMap2);
	      //####### REDUCE PHASE ###############################
	        
	        // Adding up all the values we get from the Shuffle Phase for Temperature t1 to determine the final frequency
	        int sum1 = finalMap1.values().stream().mapToInt(Number::intValue).sum();
	     // Adding up all the values we get from the Shuffle Phase for Temperature t2 to determine the final frequency
	        int sum2 = finalMap2.values().stream().mapToInt(Number::intValue).sum();
	       
	        Map<Double, Integer> finaloutput = new HashMap<>();
	        
	        finaloutput.put(t1 , sum1);
	        finaloutput.put(t2 , sum2);
	        
	        // Convert all Map keys to a List
	        List<Double> key = new ArrayList(finaloutput.keySet());

	        // Convert all Map values to a List
	        List<Integer> value = new ArrayList(finaloutput.values());
	        List<Pair> f = new ArrayList<Pair>();
	        Pair first = new Pair(key.get(0),value.get(0));
	        Pair second = new Pair(key.get(1),value.get(1));
	        
	        f.add(first);
	        f.add(second);
	        
	     // Print the value from the list....
	        for(Pair p : f) {
	        	System.out.println(p.getFirst() + "," + p.getSecond());;
	        }
	        
	        //System.out.println("Final Output  " + finaloutput);
	 }
	 //############## CREATING a KEY-VALUE PAIR INNER CLASS ################3
	 static class Pair<F, S> {
		 	private F first; //first member of pair
		    private S second; //second member of pair

			public Pair(F first, S second) {
		        this.first = first;
		        this.second = second;
		    }
		    public void setFirst(F first) {
		        this.first = first;
		    }
		    public void setSecond(S second) {
		        this.second = second;
		    }
		    public F getFirst() {
		        return first;
		    }
		    public S getSecond() {
		        return second;
		    }
		}

}
