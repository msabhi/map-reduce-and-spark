package com.pagerank.domain.preprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class IndexBuilderMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	// Inmapper combining to reduce the amount of data going out of mapper to reducer
	public Set<String> uniquePages = new HashSet<>();
	
	/*
	 * For every line in the adjacency list and the page id
	 * the function emits the node id
	 */
	
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		
		String[] list = value.toString().split("~~~");
		uniquePages.add(list[0].trim());
		if(list.length>1){
			for(String everyAdjNode : list[1].split(",")){
				uniquePages.add(everyAdjNode.trim());
			}
		}
	}	
	
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		for(String nodes : uniquePages){
			context.write(new Text(nodes), NullWritable.get());
		}
	}
}
