package com.pagerank.domain.calculate;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RowByColumnReducer extends Reducer<IntWritable, DoubleWritable, Text, Text>{
	
	
	// Aggregate every row of the new rank vector
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double pagerank = 0.0;
		for(DoubleWritable ranks : values){
			pagerank += ranks.get();
		}
		context.write(new Text(key.get()+""), new Text(pagerank+""));
	}
}
