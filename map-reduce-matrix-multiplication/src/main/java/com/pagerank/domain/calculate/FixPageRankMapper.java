package com.pagerank.domain.calculate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pagerank.driver.Constants;
import com.pagerank.driver.CounterVariables;

public class FixPageRankMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
	
	private HashMap<Integer, Double> pageRankMap = new HashMap<>();
	
	private double sinkNodePageRank = 0.0; 
	private double dampeningFactor = 0.0;
	private double noOfNodes = 0.0;
	
	// Load the temporary rank file into the memory from distributed cache
	// initialize the no. of nodes for dampening factor and dangling node loss
	
	protected void setup(Context context) throws IOException, InterruptedException {
		noOfNodes = (double)context.getConfiguration().getLong("NO_OF_NODES", 0);
		dampeningFactor = 0.15/noOfNodes;
		sinkNodePageRank = context.getConfiguration().getLong("SINK_PAGE_RANK", 0) / (double) Math.pow(10, 8);
		URI[] uri = context.getCacheFiles();
        if(uri != null ) {
            for (URI uriElement : uri) {
            	Path awsPath = new Path(Constants.EMR_FOLDER+uriElement.getPath());
            	FileSystem fs = FileSystem.get(awsPath.toUri(), context.getConfiguration());
            	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
            	//BufferedReader br = new BufferedReader(new FileReader(Constants.EMR_FOLDER+uriElement.getPath()));
        		String line = null;
        		while((line=br.readLine())!=null){
        			String[] index = line.split(":");
        			pageRankMap.put(Integer.parseInt(index[0]), Double.parseDouble(index[1]));
        		}
        		br.close();
            }
        }		
	}
	/*
	 * (non-Javadoc)
	 * For every rowid in the matrix file, if its not in the rank vetor, its a edge node, so restore it
	 * Add dampeningFactor and dangling node loss from previous step to the edge node and emit it
	 * if its in the rank vector, just add the dampening factor and dangling loss
	 */
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		String[] idAdjNodes = value.toString().split(":");
		int rowId = Integer.parseInt(idAdjNodes[0]);
		// check for non-edge node
		if(pageRankMap.containsKey(rowId)){
			output.write(new IntWritable(rowId), new DoubleWritable(dampeningFactor + 0.85* (pageRankMap.get(rowId)+sinkNodePageRank/noOfNodes)));
			//output.write(new IntWritable(rowId), new DoubleWritable(pageRankMap.get(rowId)));
		}
		else{
			// if we are here, its edge node.
			//output.write(new IntWritable(rowId), new DoubleWritable(dampeningFactor + 0.85 * sinkNodePageRank/noOfNodes));
			output.write(new IntWritable(rowId), new DoubleWritable(dampeningFactor + 0.85 * sinkNodePageRank/noOfNodes));
		}
	}
}
