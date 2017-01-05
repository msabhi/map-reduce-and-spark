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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pagerank.driver.Constants;
import com.pagerank.driver.CounterVariables;

public class RowByColumnMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
	
	// load the rank vector of previous iteration into the memory
	private HashMap<Integer, Double> pageRankMap = new HashMap<>();

	/*
	 * (non-Javadoc)
	 * read the rank file of previous iteration into the memory
	 * frmo the distributed cache
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		
		URI[] uri = context.getCacheFiles();
        if(uri != null ) {
            for (URI uriElement : uri) {
            	Path awsPath = new Path(Constants.EMR_FOLDER+uriElement.getPath());
            	FileSystem fs = FileSystem.get(awsPath.toUri(), context.getConfiguration());
            	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
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
	 * Do a multiplication of every column with its corresponding row in rank vector
	 * reads the matrix line by line and determine column of every adjacent node 
	 * for every column and multiply with row id of the pageId in rank vector

	 */
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		String[] idAdjNodes = value.toString().split(":");
		int rowId = Integer.parseInt(idAdjNodes[0]);
		double currpageRank =  pageRankMap.get(rowId);
		if(idAdjNodes.length==1){
			output.getCounter(CounterVariables.SINK_PAGE_RANK).increment((long)(pageRankMap.get(rowId) * Math.pow(10, 8)));
		}
		else{
			String[] adjNodes = idAdjNodes[1].split("#");
			for(String everyNode : adjNodes){
				int col = Integer.parseInt(everyNode.split(",")[0]);
				double contribution = Double.parseDouble(everyNode.split(",")[1]) * currpageRank;
				output.write(new IntWritable(col), new DoubleWritable(contribution));
			}
		}
	}
}
