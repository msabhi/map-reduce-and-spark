package com.hadoop.practice.salescounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

// A mapper to emit the key as the country name and 
// value as one. one being the count of the product found in that
// specific country

public class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value,
			Context output)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		output.write(new Text(tokens[7]), one);		
	}

}
