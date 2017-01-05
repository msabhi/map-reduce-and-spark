package com.mapreduce.hw2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mapreduce.hw2.domain.CompositeKeyComparator;
import com.mapreduce.hw2.domain.KeyPartitioner;
import com.mapreduce.hw2.domain.NaturalKeyGroupComparator;
import com.mapreduce.hw2.domain.TemperatureMappers;
import com.mapreduce.hw2.domain.TemperatureReducers;
import com.mapreduce.hw2.valueobjects.TempValueWritable;

/*
 * Driver program for secondary sort belonging to part 2 question
 */
public class SecondarySortDriver {

	public static void main(String[] args) throws Exception{
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: SecondarySortDriver <in> <out>");
			System.exit(2);
		}
		Job job =  Job.getInstance(conf, "Temperature counter");	
		job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setJarByClass(SecondarySortDriver.class);
		job.setMapperClass(TemperatureMappers.class);
		job.setReducerClass(TemperatureReducers.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setNumReduceTasks(10);
		job.setMapOutputValueClass(TempValueWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path output = new Path(otherArgs[1]);
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
