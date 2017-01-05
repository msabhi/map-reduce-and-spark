package com.mapreduce.hw2.domain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.mapreduce.hw2.valueobjects.TempValueWritable;

public class TemperatureMappers extends Mapper<LongWritable, Text, Text, TempValueWritable>{
	
	// In mapper combiner data structure 
	private Map<String, TempValueWritable> maxMemoryMap = new HashMap<String, TempValueWritable>();
	
	
	/*checkInMapAndUpdate : line
	 * 1> Checks if the line belongs to TMAX/TMIN
	 * 2> Updates the TMAX/TMIN information to the object accordingly
	 */
	private void checkInMapAndUpdate(String[] tokens){
		String type = tokens[2].trim();
		int year = Integer.parseInt(tokens[1].substring(0,4));
		String stationId = tokens[0];
		String mapkey = stationId + "-" + year;			
		int value = Integer.parseInt(tokens[3]);
		if(type.equals("TMIN")){
			
			// Already exists in map, so just update
			if(maxMemoryMap.containsKey(mapkey)){
				TempValueWritable tw = maxMemoryMap.get(mapkey);
				tw.setNoOfTminStations(tw.getNoOfTminStations()+1);
				tw.setTMIN(tw.getTMIN()+value);
			}
			else{
				// initialize the year object for the station id
				TempValueWritable tw = new TempValueWritable(year,0,0,0,0);
				tw.setNoOfTminStations(1);
				tw.setTMIN(value);
				maxMemoryMap.put(mapkey, tw);
			}
			
		}
		else{
			// Already exists in map, so just update
			if(maxMemoryMap.containsKey(mapkey)){
				TempValueWritable tw = maxMemoryMap.get(mapkey);
				tw.setNoOfTmaxStations(tw.getNoOfTmaxStations()+1);
				tw.setTMAX(tw.getTMAX()+value);
			}
			// initialize the year object for the station id
			else{
				TempValueWritable tw = new TempValueWritable(year,0,0,0,0);
				tw.setNoOfTmaxStations(1);
				tw.setTMAX(value);
				maxMemoryMap.put(mapkey, tw);
			}
		}
	}

	/*
	 * Since its a in mapper combiner the map function would just aggregate 
	 * all the equivalent station ids 
	 */
	public void map(LongWritable key, Text line,Context output) throws IOException, InterruptedException {
		String[] tokens = line.toString().split(",");
		String type = tokens[2].trim();
		if(type.equals("TMIN") || type.equals("TMAX")){
			checkInMapAndUpdate(tokens);
		}

	}

	/*
	 * Finally, emit the key value pairs once all the map tasks are 
	 * completed
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(String keys : maxMemoryMap.keySet()){
			System.out.println("context writer"+maxMemoryMap.get(keys));
			/*
			 * MAPPER OUTPUT : 
			 * 1> Composite key is a "-" delimited Text(String)
			 *    Natural key = station id
			 *    Natural value = year 
			 * 2> Value is a Custom writable object having TMAX/TMIN/noOfStations values
			 */
			context.write(new Text(keys), maxMemoryMap.get(keys));
		}
	}
}
