package com.mapreduce.hw2.domain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.mapreduce.hw2.valueobjects.TemperatureWritable;

// This mapper belongs to the 1.C part of the question
public class TempInMapperCombiner extends Mapper<LongWritable, Text, Text, TemperatureWritable>{

	// Inmapper memory data structure to store aggregated MAX value objects
	private Map<String, TemperatureWritable> tmaxInmapperMemory = new HashMap<String, TemperatureWritable>();
	
	// Inmapper memory data structure to store aggregated MIN value objects
	private Map<String, TemperatureWritable> tminInmapperMemory = new HashMap<String, TemperatureWritable>();

	public void map(LongWritable key, Text value,
			Context output)
					throws IOException, InterruptedException {

		// retrieve the CSV line value and split it on "," 
		String[] tokens = value.toString().split(",");
		
		// If the line belongs to TMIN type, loaded it to the tmin MAP
		if(tokens[2].trim().equals("TMIN") ) {
			String temperature = tokens[3].trim().equals("")? "0" : tokens[3].trim();
			if(tminInmapperMemory.containsKey(tokens[0])){
				tminInmapperMemory.get(tokens[0]).update(Integer.parseInt(tokens[3]), 1);
			}
			else{
				TemperatureWritable tw = new TemperatureWritable(new Text(tokens[2].trim()), 
						new IntWritable(Integer.parseInt(temperature)));
				tminInmapperMemory.put(tokens[0], tw);
			}
		}

		// If the line belongs to TMIN type, loaded it to the tmax MAP
		if(tokens[2].trim().equals("TMAX")){
			String temperature = tokens[3].trim().equals("")? "0" : tokens[3].trim();
			if(tmaxInmapperMemory.containsKey(tokens[0])){
				tmaxInmapperMemory.get(tokens[0]).update(Integer.parseInt(tokens[3]), 1);
			}
			else{
				TemperatureWritable tw = new TemperatureWritable(new Text(tokens[2].trim()), 
						new IntWritable(Integer.parseInt(temperature)));
				tmaxInmapperMemory.put(tokens[0], tw);
			}
		}
	}

	// Cleanup method provides the way to emit the aggregated hash map key values
	// This runs once for every mapper
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(String stationId : tmaxInmapperMemory.keySet()){
			context.write(new Text(stationId), tmaxInmapperMemory.get(stationId));
		}
		for(String stationId : tminInmapperMemory.keySet()){
			context.write(new Text(stationId), tminInmapperMemory.get(stationId));
		}
	}

}
