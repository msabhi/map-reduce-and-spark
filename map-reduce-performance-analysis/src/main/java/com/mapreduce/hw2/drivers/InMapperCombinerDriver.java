package com.mapreduce.hw2.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mapreduce.hw2.domain.TempInMapperCombiner;
import com.mapreduce.hw2.domain.TemperatureCombiner;
import com.mapreduce.hw2.domain.TemperatureMapper;
import com.mapreduce.hw2.domain.TemperatureReducer;
import com.mapreduce.hw2.valueobjects.TemperatureWritable;

/*
 * In mapper combiner driver program
 */
public class InMapperCombinerDriver {

	public static void main(String[] args) throws Exception{
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: InMapperCombinerDriver <in> <out>");
			System.exit(2);
		}
		Job job =  Job.getInstance(conf, "product counter");	
		job.setJarByClass(InMapperCombinerDriver.class);
		job.setMapperClass(TempInMapperCombiner.class);
		job.setReducerClass(TemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TemperatureWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path output = new Path(otherArgs[1]);
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
