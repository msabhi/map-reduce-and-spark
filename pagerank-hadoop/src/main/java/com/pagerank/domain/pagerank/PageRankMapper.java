package com.pagerank.domain.pagerank;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Counter;

import com.pagerank.valueobjects.PageNode;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, PageNode>{
	public void map(LongWritable key, Text value,
			Context output)
					throws IOException, InterruptedException {

		// Get the number of page nodes in the graph
		double noOfPageNodes = output.getConfiguration().getLong("NO_OF_NODES", 0);
		double sinkNodePageRank = output.getConfiguration().getLong("SINK_PAGE_RANK", 0) / (double) Math.pow(10, 8);


		/*
		 * Parse the line to get the page node name, page rank,
		 * and adjacency list 
		 */
		String[] pgNameRankAdjList = value.toString().split("~~~");

		String pgName;  // page node name
		double pgRank;						   // page rank
		String[] adjNodes;					   // adjacency list

		//If first iteration, set the page rank to 1/N
		if(pgNameRankAdjList[0].trim().equals("INITIAL")){
			pgRank = 1 / noOfPageNodes;
			if(pgNameRankAdjList.length==2){
				adjNodes = new String[0];
			}
			else{
				adjNodes = pgNameRankAdjList[2].split(",");
			}
			pgName = pgNameRankAdjList[1];
		}
		else{
			pgName = pgNameRankAdjList[0];
			pgRank = Double.parseDouble(pgNameRankAdjList[1]);
			if(pgNameRankAdjList.length==3){
				adjNodes = pgNameRankAdjList[2].split(",");
			}
			else{
				adjNodes = new String[0];
			}
		}


		/*
		 * calculate the previous iteration sink node page rank loss
		 * by fetching the previous iteration and adding it to the current
		 * execution node page rank
		 */
		double deltaLoss = sinkNodePageRank / noOfPageNodes; 
		pgRank = pgRank + deltaLoss;


		PageNode pgNode = new PageNode(pgRank, adjNodes, true);

		//write the graph to the output so we save the graph for further
		//iterations

		output.write(new Text(pgName), pgNode);
		/*
		 * emit the page rank to
		 * all of its adjacent nodes 
		 */
		double pgRankPerNode = pgRank/adjNodes.length; 
		for(String adjPgNode:adjNodes){
			output.write(new Text(adjPgNode), new PageNode(pgRankPerNode, new String[0], false));
		}

	}


}
