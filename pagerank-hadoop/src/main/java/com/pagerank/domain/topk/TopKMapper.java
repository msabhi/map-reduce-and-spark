package com.pagerank.domain.topk;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pagerank.valueobjects.PageEntries;


public class TopKMapper extends Mapper<LongWritable, Text, PageEntries, DoubleWritable> {
	
	// queue to store the sorted page ranks 
	Queue<PageEntries> queueOfPageEntries = new PriorityQueue<>();
	
	public void map(LongWritable key, Text value,
			Context output)
					throws IOException, InterruptedException {
		String[] pgNameRank = value.toString().split("~~~");
		
		//Calculate the delta loss of 10th iteration
		long noOfNodes = output.getConfiguration().getLong("NO_OF_NODES", 0);
		double sinkNodePageRank = output.getConfiguration().getLong("SINK_PAGE_RANK", 0) / (double) Math.pow(10, 8);
		double deltaLoss = sinkNodePageRank / (double) noOfNodes;
		
		queueOfPageEntries.offer(new PageEntries(pgNameRank[0], Double.parseDouble(pgNameRank[1]) + deltaLoss));
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		int i = 99;
		//emit only the top 100 of the page ranks
		while(i>=0 && !queueOfPageEntries.isEmpty()){
			PageEntries pgEntry = queueOfPageEntries.poll();
			context.write(pgEntry, new DoubleWritable(pgEntry.pageRank));
			i--;
		}
	}
	

}