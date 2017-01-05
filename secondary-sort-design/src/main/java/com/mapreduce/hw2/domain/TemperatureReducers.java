package com.mapreduce.hw2.domain;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.mapreduce.hw2.valueobjects.TempValueWritable;

/*
 * Reducer to aggregate the TMAX/TMIN for all the station ids
 */
public class TemperatureReducers extends Reducer<Text, TempValueWritable, Text, Text> {
	
	/*
	 * Composite Key : Text
	 * 		1> Natural key : Station Id
	 * 		2> Natural Value : Year
	 * 
	 * Value is a iterable of all the years (custom writable object) in the ascending order.
	 * 
	 * The function won't be using any in memory map to sort/aggregate.
	 * 
	 * Aggregation is done sequentially and values for every year is finalized
	 * whenever new year is encountered.
	 */
	public void reduce(Text key, Iterable<TempValueWritable> values, Context context) throws IOException, InterruptedException {
		
		//Initialize our result object
		StringBuilder resultValue = new StringBuilder("[");
		
		//Our local aggregator object
		TempValueWritable result = new TempValueWritable(0, 0, 0, 0, 0);
		
		for(TempValueWritable tw : values){
			
			//If this is the first year, lets save it to our aggregator object
			if(result.getYear()==0) result.setYear(tw.getYear());
			
			// If the year is still equal, we are supposed to just update our accumulator object
			if(tw.getYear()==result.getYear()){
				result.setNoOfTmaxStations(tw.getNoOfTmaxStations()+result.getNoOfTmaxStations());
				result.setNoOfTminStations(tw.getNoOfTminStations()+result.getNoOfTminStations());
				result.setTMAX(tw.getTMAX()+result.getTMAX());
				result.setTMIN(tw.getTMIN()+result.getTMIN());
			}
			// if we are here, we are having a new year. Lets calculate the averages
			// and append the result to our result string.
			else{
				double maxavg = result.getTMAX() / (double) result.getNoOfTmaxStations();
				double minavg = result.getTMIN() / (double) result.getNoOfTminStations();
				resultValue.append("(" + result.getYear() + "," + minavg + "," + maxavg + "),");
				//Lets reintiailze our result accumulator object to work on the next year
				result = new TempValueWritable(tw.getYear(), tw.getTMAX(), tw.getNoOfTmaxStations(), tw.getTMIN(), tw.getNoOfTminStations());
			}
		}
		
		// Lets calculate the last year in the list
		double maxavg = result.getTMAX() / (double) result.getNoOfTmaxStations();
		double minavg = result.getTMIN() / (double) result.getNoOfTminStations();
		resultValue.append("("+result.getYear() + "," + minavg + "," + maxavg+")]");
		
		//output the final value where
		// Key is a Text - station id
		// Value is Text containing the list of years in descending order and their
		// minimum and maximum averages
		context.write(new Text(key.toString().split("-")[0]), new Text(resultValue.toString()));
	}
}
