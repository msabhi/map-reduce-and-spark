package com.pagerank.driver;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pagerank.domain.calculate.FixPageRankMapper;
import com.pagerank.domain.calculate.RowByColumnMapper;
import com.pagerank.domain.calculate.RowByColumnReducer;
import com.pagerank.domain.preprocessor.IndexBuilderMapper;
import com.pagerank.domain.preprocessor.IndexBuilderReducer;
import com.pagerank.domain.preprocessor.MatrixBuilderMapper;
import com.pagerank.domain.preprocessor.MatrixBuilderReducer;
import com.pagerank.domain.preprocessor.RankBuilderMapper;
import com.pagerank.domain.topk.PageEntries;
import com.pagerank.domain.topk.TopKKeyPartitioner;
import com.pagerank.domain.topk.TopKMapper;
import com.pagerank.domain.topk.TopKReducer;


public class PageRankChainedDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{

		
		// Run the pre-processing mapper
		Configuration config = new Configuration();
		System.setProperty("hadoop.home.dir", "/");
		config.set("mapred.textoutputformat.separator", "~~");
		Job indexBuilderJob = Job.getInstance(config, "Index builder");	
		FileInputFormat.addInputPath(indexBuilderJob, new Path(args[0]+"/input"));
		Path out = new Path(args[0]+"/index");
		FileOutputFormat.setOutputPath(indexBuilderJob, out);
		out.getFileSystem(config).delete(out, true);
		indexBuilderJob.setJarByClass(PageRankChainedDriver.class);
		indexBuilderJob.setMapperClass(IndexBuilderMapper.class);
		indexBuilderJob.setReducerClass(IndexBuilderReducer.class);
		indexBuilderJob.setMapOutputKeyClass(Text.class);
		indexBuilderJob.setMapOutputValueClass(NullWritable.class);
		indexBuilderJob.setNumReduceTasks(1);
		indexBuilderJob.waitForCompletion(true);
		long noOfNodes = indexBuilderJob.getCounters().findCounter(CounterVariables.NO_OF_PAGE_NODES).getValue();
		System.out.println(noOfNodes);
		//job.addCacheFile(new Path(filename).toUri());

		// Set the number of nodes counted in the preprocessing mapper
		config.setLong("NO_OF_NODES", noOfNodes);

		Job matrixBuilderJob = Job.getInstance(config, "Matrix builder");	
		FileInputFormat.addInputPath(matrixBuilderJob, new Path(args[0]+"/input"));
		out = new Path(args[0]+"/matrix");
		FileOutputFormat.setOutputPath(matrixBuilderJob, out);
		out.getFileSystem(config).delete(out, true);
		matrixBuilderJob.setJarByClass(PageRankChainedDriver.class);
		matrixBuilderJob.setMapperClass(MatrixBuilderMapper.class);
		matrixBuilderJob.setReducerClass(MatrixBuilderReducer.class);
		matrixBuilderJob.setMapOutputKeyClass(Text.class);
		matrixBuilderJob.setMapOutputValueClass(NullWritable.class);
		matrixBuilderJob.setNumReduceTasks(1);
		Path directoryPath = new Path(new URI(args[0]+"/index"));	
        FileSystem fs = directoryPath.getFileSystem(config);
        FileStatus[] fileStatus = fs.listStatus(directoryPath);
        for (FileStatus status : fileStatus) {
        	matrixBuilderJob.addCacheFile(status.getPath().toUri());
        }
		//matrixBuilderJob.addCacheFile(new Path(args[0]+"/index/part-r-00000").toUri());
		matrixBuilderJob.waitForCompletion(true);

		Job rankBuilderJob = Job.getInstance(config, "Rank builder");	
		FileInputFormat.addInputPath(rankBuilderJob, new Path(args[0]+"/matrix"));
		out = new Path(args[0]+"/rank");
		FileOutputFormat.setOutputPath(rankBuilderJob, out);
		out.getFileSystem(config).delete(out, true);
		rankBuilderJob.setJarByClass(PageRankChainedDriver.class);
		rankBuilderJob.setMapperClass(RankBuilderMapper.class);
		rankBuilderJob.setMapOutputKeyClass(Text.class);
		rankBuilderJob.setMapOutputValueClass(NullWritable.class);
		rankBuilderJob.setNumReduceTasks(0);
		rankBuilderJob.waitForCompletion(true);
		config.set("mapred.textoutputformat.separator", ":");
		
		// 1st iteration
		for(int i=1;i<=10;i++){
			// 1st iteration		
			config.set("mapred.textoutputformat.separator", ":");
			Job calculateJob = Job.getInstance(config, "Calculate page rank");	
			FileInputFormat.addInputPath(calculateJob, new Path(args[0]+"/matrix"));
			directoryPath = new Path(new URI(args[0]+"/rank"));
	        fs = directoryPath.getFileSystem(config);
	        fileStatus = fs.listStatus(directoryPath);
	        for (FileStatus status : fileStatus) {
	        	calculateJob.addCacheFile(status.getPath().toUri());
	        }
			//calculateJob.addCacheFile(new Path(args[0]+"/rank/part-m-00000").toUri());
			out = new Path(args[0]+"/temprank");
			FileOutputFormat.setOutputPath(calculateJob, out);
			out.getFileSystem(config).delete(out, true);
			calculateJob.setJarByClass(PageRankChainedDriver.class);
			calculateJob.setMapperClass(RowByColumnMapper.class);
			calculateJob.setReducerClass(RowByColumnReducer.class);
			calculateJob.setMapOutputKeyClass(IntWritable.class);
			calculateJob.setMapOutputValueClass(DoubleWritable.class);
			calculateJob.waitForCompletion(true);
			config.setLong("SINK_PAGE_RANK", calculateJob.getCounters().findCounter(CounterVariables.SINK_PAGE_RANK).getValue());
			calculateJob.getCounters().findCounter(CounterVariables.SINK_PAGE_RANK).setValue(0);

			// Post processing job 2
			Job fixPageRankJob = Job.getInstance(config, "Fix the page rank for iteration = " + i);	
			FileInputFormat.addInputPath(fixPageRankJob, new Path(args[0]+"/matrix"));
			directoryPath = new Path(new URI(args[0]+"/temprank"));
	        fs = directoryPath.getFileSystem(config);
	        fileStatus = fs.listStatus(directoryPath);
	        for (FileStatus status : fileStatus) {
	        	fixPageRankJob.addCacheFile(status.getPath().toUri());
	        }
			//fixPageRankJob.addCacheFile(new Path(args[0]+"/temprank/part-r-00000").toUri());
			out = new Path(args[0]+"/rank");
			FileOutputFormat.setOutputPath(fixPageRankJob, out);
			out.getFileSystem(config).delete(out, true);
			fixPageRankJob.setJarByClass(PageRankChainedDriver.class);
			fixPageRankJob.setMapperClass(FixPageRankMapper.class);
			fixPageRankJob.setMapOutputKeyClass(IntWritable.class);
			fixPageRankJob.setMapOutputValueClass(DoubleWritable.class);
			fixPageRankJob.setNumReduceTasks(0);
			fixPageRankJob.waitForCompletion(true);
		}
		
		
		config.set("mapreduce.textoutputformat.separator", ",");
		Job topKJob = Job.getInstance(config, "Select the TOP K ranked pages");
		Path in = new Path(args[0]+"rank");
		FileInputFormat.addInputPath(topKJob, in);
		Path outPath = new Path(args[0]+"topk");
		directoryPath = new Path(new URI(args[0]+"/index"));
        fs = directoryPath.getFileSystem(config);
        fileStatus = fs.listStatus(directoryPath);
        for (FileStatus status : fileStatus) {
        	topKJob.addCacheFile(status.getPath().toUri());
        }
		//topKJob.addCacheFile(new Path(args[0]+"/index/part-r-00000").toUri());
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
		topKJob.setNumReduceTasks(1);
		topKJob.waitForCompletion(true);
		
	}
}
