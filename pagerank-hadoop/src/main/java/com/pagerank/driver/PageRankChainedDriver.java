package com.pagerank.driver;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pagerank.domain.pagerank.CounterVariables;
import com.pagerank.domain.pagerank.PageRankMapper;
import com.pagerank.domain.pagerank.PageRankMapperParserV1;
import com.pagerank.domain.pagerank.PageRankReducer;
import com.pagerank.domain.pagerank.PageRankReducerParser;
import com.pagerank.domain.topk.NaturalKeyGroupComparator;
import com.pagerank.domain.topk.TopKKeyPartitioner;
import com.pagerank.domain.topk.TopKMapper;
import com.pagerank.domain.topk.TopKReducer;
import com.pagerank.valueobjects.PageEntries;
import com.pagerank.valueobjects.PageNode;
public class PageRankChainedDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		// Run the pre-processing mapper
		Configuration config = new Configuration();
		System.setProperty("hadoop.home.dir", "/");
		Job preprocessingJob = Job.getInstance(config, "Preprocessing mapper");
		FileInputFormat.addInputPath(preprocessingJob, new Path(args[0]));
		Path out = new Path(args[1]+"iter0");
		FileOutputFormat.setOutputPath(preprocessingJob, out);
		out.getFileSystem(config).delete(out, true);
		preprocessingJob.setJarByClass(PageRankChainedDriver.class);
		preprocessingJob.setMapperClass(PageRankMapperParserV1.class);
		preprocessingJob.setReducerClass(PageRankReducerParser.class);
		preprocessingJob.setMapOutputKeyClass(Text.class);
		preprocessingJob.setMapOutputValueClass(PageNode.class);
		preprocessingJob.waitForCompletion(true);
		long noOfNodes = preprocessingJob.getCounters().findCounter(CounterVariables.NO_OF_PAGE_NODES).getValue();
		
		// Set the number of nodes counted in the preprocessing mapper
		config.setLong("NO_OF_NODES", noOfNodes);
		
		/*
		 * Process 10 iterations to calculate the page rank
		 */
		for(int iterations = 1; iterations <= 10 ; iterations++) {
			System.setProperty("hadoop.home.dir", "/");
			Job pageRankJob = Job.getInstance(config, "page rank");
			pageRankJob.setJobName("Calculate page ranks " + iterations);
			pageRankJob.setMapperClass(PageRankMapper.class);
			pageRankJob.setReducerClass(PageRankReducer.class);
			pageRankJob.setJarByClass(PageRankChainedDriver.class);
			Path in = new Path(args[1]+"iter" + (iterations - 1) + "/");
			Path out1 = new Path(args[1]+ "iter" + iterations);
			FileInputFormat.addInputPath(pageRankJob, in);
			FileOutputFormat.setOutputPath(pageRankJob, out1);
			out1.getFileSystem(config).delete(out1, true);
			pageRankJob.setMapOutputKeyClass(Text.class);
			pageRankJob.setMapOutputValueClass(PageNode.class);
			pageRankJob.setOutputKeyClass(Text.class);
			pageRankJob.setOutputValueClass(Text.class);
			pageRankJob.waitForCompletion(true);
			config.setLong("SINK_PAGE_RANK", pageRankJob.getCounters().findCounter(CounterVariables.SINK_PAGE_RANK).getValue());
			pageRankJob.getCounters().findCounter(CounterVariables.SINK_PAGE_RANK).setValue(0);
		}

		// TOP K algorithm to fetch the top 100 page nodes
		config.set("mapreduce.textoutputformat.separator", ",");
		Job topKJob = Job.getInstance(config, "Select the TOP K ranked pages");
		Path in = new Path(args[1]+"iter10");
		FileInputFormat.addInputPath(topKJob, in);
		Path outPath = new Path(args[1]+"topk");
		FileOutputFormat.setOutputPath(topKJob, outPath);
		outPath.getFileSystem(config).delete(outPath, true);
		topKJob.setJarByClass(PageRankChainedDriver.class);
		topKJob.setMapperClass(TopKMapper.class);
		topKJob.setMapOutputKeyClass(PageEntries.class);
		topKJob.setMapOutputValueClass(DoubleWritable.class);
		topKJob.setReducerClass(TopKReducer.class);
		topKJob.setOutputKeyClass(NullWritable.class);
		topKJob.setOutputValueClass(Text.class);
		topKJob.setPartitionerClass(TopKKeyPartitioner.class);
		topKJob.setGroupingComparatorClass(NaturalKeyGroupComparator.class);
		topKJob.setNumReduceTasks(1);
		topKJob.waitForCompletion(true);
	}
}
