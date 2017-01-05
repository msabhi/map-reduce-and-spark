package com.pagerank.domain.preprocessor;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankBuilderMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	// A simple map only job to create a rank vector with 
	// initial page rank 1/NO_OF_NODES
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		String pageId = value.toString().split(":")[0];
		output.write(new Text(pageId+":"+(1/(double)output.getConfiguration().getLong("NO_OF_NODES", 0))), NullWritable.get());
	}
}
