package com.pagerank.domain.preprocessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pagerank.driver.Constants;
import com.pagerank.driver.CounterVariables;

public class MatrixBuilderReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

	// Maintain the index file in the memory
	private HashMap<Integer, String> indexMap = new HashMap<>();
	
	// Load the index file into memory from the files in distributed cache
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
        			indexMap.put(Integer.parseInt(index[1]), index[0]);
        		}
        		br.close();
            }
        }
		
	}
	
	/*
	 * Reduce handles all the secondary dangling nodes.
	 * For all the nodes emitted from the mapper, remove it 
	 * from our index memory
	 */
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		int pageId = Integer.parseInt(key.toString().split(":")[0]);
		indexMap.remove(pageId);
		context.write(NullWritable.get(), new Text(key.toString()));
	}
	
	/*
	 * left over nodes in the index memory are the ones
	 * which are currently not in our graph and are secondary dangling ndoes
	 * the function emits it as sink node
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(Integer idx:indexMap.keySet()){
			context.write(NullWritable.get(), new Text(idx+":"));
		}
	}
}
