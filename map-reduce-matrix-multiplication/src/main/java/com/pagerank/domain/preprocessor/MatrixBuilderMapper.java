package com.pagerank.domain.preprocessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pagerank.driver.Constants;

public class MatrixBuilderMapper  extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	// Maintain the index file in the memory
	
	private HashMap<String, Integer> indexMap = new HashMap<>();
	
	
	// Load the index file into the memory from the files in distributed cache
	protected void setup(Context context
            ) throws IOException, InterruptedException {
		
		URI[] uri = context.getCacheFiles();
        if(uri != null ) {
            for (URI uriElement : uri) {
            	Path awsPath = new Path(Constants.EMR_FOLDER+uriElement.getPath());
            	FileSystem fs = FileSystem.get(awsPath.toUri(), context.getConfiguration());
            	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
            	//BufferedReader br = new BufferedReader(new FileReader(Constants.EMR_FOLDER+uriElement.getPath()));
        		String line = null;
        		while((line=br.readLine())!=null){
        			String[] index = line.split("~~");
        			indexMap.put(index[0], Integer.parseInt(index[1]));
        		}
        		br.close();
            }
        }
		
	}
	
	// For every PageName and its Adjacency list page names,
	// fetch their corresponding id from the index map 
	// and convert it into sparse matrix 
	// Also include the contribution from a page name to all its pagenames in adjacency list
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		
		String[] list = value.toString().split("~~~");
		StringBuilder result = new StringBuilder();
		result.append(indexMap.get(list[0].trim())+":");
		
		// if non-sink node, include the sparse adjacency list
		if(list.length>1){
			String[] adjList = list[1].split(",");
			double size = adjList.length;
			for (String adjNode : adjList){
				result.append(indexMap.get(adjNode)+","+1/size+"#");
			}
			result.deleteCharAt(result.length()-1);
		}
		output.write(new Text(result.toString()), NullWritable.get());
	}	
	
}
