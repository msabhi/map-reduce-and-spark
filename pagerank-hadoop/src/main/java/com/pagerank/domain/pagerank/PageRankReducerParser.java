package com.pagerank.domain.pagerank;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.pagerank.valueobjects.PageNode;

public class PageRankReducerParser extends Reducer<Text, PageNode, NullWritable, Text> {
	
	
	public void reduce(Text key, Iterable<PageNode> values, Context context) throws IOException, InterruptedException {
		String initial = "INITIAL"; 
		//count the number of unique nodes
		context.getCounter(CounterVariables.NO_OF_PAGE_NODES).increment(1);
		//select the first adjacency list
		PageNode pgNode = values.iterator().next();
		List<String> adjList = Arrays.asList(pgNode.getAdjacentList());
		
		//write to the first iteration page rank mapper with INITIAL so 
		// the mapper can identify it as first iteration
		context.write(NullWritable.get(),
				new Text( initial + "~~~" + key+"~~~"+adjList.stream().collect(Collectors.joining(","))));
		}
}
