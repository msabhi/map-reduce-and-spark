package com.pagerank.domain.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.pagerank.valueobjects.PageNode;

public class PageRankReducer extends Reducer<Text, PageNode, NullWritable, Text> {

	public void reduce(Text key, Iterable<PageNode> values, Context context) throws IOException, InterruptedException {

		String pageName = key.toString();
		PageNode pageNode = null;
		double pageRank = 0.0;
		boolean isPrimaryOrSinkNode = false;
		double dampeningFactor = 0.15;
		double noOfPageNodes = context.getConfiguration().getLong("NO_OF_NODES", 0);

		// aggregate the contributions to this node from the others
		for(PageNode everyPageNode : values){
			isPrimaryOrSinkNode = isPrimaryOrSinkNode || everyPageNode.getIsPrimaryOrSinkNode();
			
			
			if(isPrimaryOrSinkNode && pageNode == null){
				pageNode = new PageNode(everyPageNode.getPageRank(), everyPageNode.getAdjacentList(), true);
			}
			else{
				//node from the graph structure
				pageRank += everyPageNode.getPageRank();
			}
		}
		
		//  add the dampening factor to the total page rank
		pageRank = dampeningFactor / noOfPageNodes + (1-dampeningFactor) * pageRank;
		
		if(pageNode!=null){
			//if sink node update the counter for sink and dangling nodes
			if(pageNode.getAdjacentList().length==0){
				//context.getCounter(CounterVariables.SINK_PAGE_RANK).increment(Double.doubleToLongBits(pageRank));
				context.getCounter(CounterVariables.SINK_PAGE_RANK).increment((long)(pageRank * Math.pow(10, 8)));
			}

			pageNode.setPageRank(pageRank);
			//write the page rank and page name
			context.write(NullWritable.get(), new Text(pageName + "~~~" + pageNode.toString()));
		}
		else{
			// if secondary node, update the page rank
			context.getCounter(CounterVariables.SINK_PAGE_RANK).increment((long)(pageRank * Math.pow(10, 8)));
		}
	}
}
