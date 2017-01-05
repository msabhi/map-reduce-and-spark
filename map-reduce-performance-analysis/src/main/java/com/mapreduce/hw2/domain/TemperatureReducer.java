package com.mapreduce.hw2.domain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.mapreduce.hw2.valueobjects.TemperatureWritable;

// A general reducer class for all 1.a,1.b,1.c parts of the question

public class TemperatureReducer extends Reducer<Text, TemperatureWritable, Text, Text>{
	
	/*
	 * (non-Javadoc)
	 * Reduce method handles the aggregation and emitting of the final reduce task
	 * to the parts file 
	 */
	public void reduce(Text key, Iterable<TemperatureWritable> values, Context context) throws IOException, InterruptedException {
		
		// Initialize the local variables
		long tmax = 0; long tmin = 0;
		int noOfTmaxStations = 0, noOfTminStations = 0;
		
		// Initialize the reduce key
		Text result = new Text();
		
		// For every value in iterables, aggregate TMAX/TMIN values
		for(TemperatureWritable tw : values){
			if(tw.getType().toString().equals("TMAX")){
				tmax += tw.getValue().get();
				noOfTmaxStations += tw.getNoOfStations().get();
			}
			else{
				tmin += tw.getValue().get();
				noOfTminStations += tw.getNoOfStations().get();
			}
		}
		
		//Calculate the averages
		double avgtmax =  tmax /(double)  noOfTmaxStations;
		double avgtmin =  tmin / (double) noOfTminStations;
		result.set(avgtmin + "," + avgtmax);
		
		//Emit the final result	
		context.write(key, result);
	}
}
