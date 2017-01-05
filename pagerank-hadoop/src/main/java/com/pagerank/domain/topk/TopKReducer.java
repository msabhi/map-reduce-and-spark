package com.pagerank.domain.topk;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.pagerank.valueobjects.PageEntries;

public class TopKReducer extends Reducer<PageEntries, DoubleWritable, NullWritable, Text>{
	
	
	/*
	 * the iterable values are sorted in descending order of page ranks
	 * the function emits the top 100
	 */
	public void reduce(PageEntries key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for(DoubleWritable value : values){
			String pgName = key.pageName;
			context.write(NullWritable.get(), new Text(pgName+" , " +value.get()+""));
			i++;
			if(i == 100) break;
		}
	}
	

}
