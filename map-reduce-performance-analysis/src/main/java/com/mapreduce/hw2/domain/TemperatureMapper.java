package com.mapreduce.hw2.domain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.mapreduce.hw2.valueobjects.TemperatureWritable;

/*
 * Mapper job for 1.a & 1.b part of the program
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, TemperatureWritable>{

	
	/*
	 * (non-Javadoc)
	 * read every line, split it into tokens 
	 * and emit the key value pair
	 */
	public void map(LongWritable key, Text value,
			Context output)
					throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		if(tokens[2].trim().equals("TMIN") || tokens[2].trim().equals("TMAX")) {
			String temperature = tokens[3].trim().equals("")? "0" : tokens[3].trim();
			TemperatureWritable tw = new TemperatureWritable(new Text(tokens[2].trim()), 
					new IntWritable(Integer.parseInt(temperature)));
			output.write(new Text(tokens[0]), tw);
		}
	}

}
