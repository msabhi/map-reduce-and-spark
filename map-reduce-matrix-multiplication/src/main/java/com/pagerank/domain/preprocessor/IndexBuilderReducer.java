package com.pagerank.domain.preprocessor;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.pagerank.driver.CounterVariables;


public class IndexBuilderReducer extends Reducer<Text, NullWritable, Text, Text> {

	// keep track of the index of every node encountered
	public static int counter = 0;
	
	/*
	 * For every unique node received, emit it with its current index
	 * increment the index
	 */
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(new Text(key), new Text(counter+""));
		context.getCounter(CounterVariables.NO_OF_PAGE_NODES).increment(1);
		counter++;
	}
}
