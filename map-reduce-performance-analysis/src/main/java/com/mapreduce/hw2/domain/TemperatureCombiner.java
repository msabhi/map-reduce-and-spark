package com.mapreduce.hw2.domain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.mapreduce.hw2.valueobjects.TemperatureWritable;

/*
 * Combiner for the 2nd program
 * This is different from the reducer. In this it adds up the no of MAX/MIN stations
 * per mapper it is assigned to
 */
public class TemperatureCombiner extends Reducer<Text, TemperatureWritable, Text, TemperatureWritable>{
	public void reduce(Text key, Iterable<TemperatureWritable> values, Context context) throws IOException, InterruptedException {		
		// Initialize local variables
		int tmax = 0; int tmin = 0;
		int noOfTmaxStations = 0, noOfTminStations = 0;
		
		// For every value retrieved from mapper 
		// aggregate the tmax/tmin temperatures for every station
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
		
		//Create TMAX/TMIN objects and emit the same
		TemperatureWritable tmaxwritable = new TemperatureWritable();
		tmaxwritable.setNoOfStations(new IntWritable(noOfTmaxStations));
		tmaxwritable.setType(new Text("TMAX"));
		tmaxwritable.setValue(new IntWritable(tmax));
		context.write(key, tmaxwritable);
		TemperatureWritable tminwritable = new TemperatureWritable();
		tminwritable.setNoOfStations(new IntWritable(noOfTminStations));
		tminwritable.setType(new Text("TMIN"));
		tminwritable.setValue(new IntWritable(tmin));
		context.write(key, tminwritable);
	}
}
