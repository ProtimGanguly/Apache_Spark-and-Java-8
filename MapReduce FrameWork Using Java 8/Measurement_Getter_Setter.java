package mapReduce;

public class Measurement {
	//########### Contains the getters and setter for fetching the details of WeatherStation
	private int time;
	private double temperature;
	public Measurement(int time , double temperature)
	{
		this.time = time;
		this.temperature = temperature;
	}
	public int getTime() {
		return time;
	}
	public double getTemperature() {
		return temperature;
	}
	
}
