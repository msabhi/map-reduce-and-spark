package com.pagerank.domain.topk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.pagerank.driver.Constants;


public class TopKReducer extends Reducer<PageEntries, DoubleWritable, NullWritable, Text>{
	
	private HashMap<String, String> pageRankMap = new HashMap<>();
	protected void setup(Context context
            ) throws IOException, InterruptedException {
		URI[] uri = context.getCacheFiles();
        if(uri != null ) {
            for (URI uriElement : uri) {
            	Path awsPath = new Path(Constants.EMR_FOLDER+uriElement.getPath());
            	FileSystem fs = FileSystem.get(awsPath.toUri(), context.getConfiguration());
            	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
        		String line = null;
        		while((line=br.readLine())!=null){
        			String[] index = line.split("~~");
        			pageRankMap.put(index[1], index[0]);
        		}
        		br.close();
            }
        }		
	}
	
	/*
	 * the iterable values are sorted in descending order of page ranks
	 * the function emits the top 100
	 */
	public void reduce(PageEntries key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for(DoubleWritable value : values){
			String pgName = key.pageName;
			context.write(NullWritable.get(), new Text(pageRankMap.get(pgName)+" , " +value.get()+""));
			i++;
			if(i == 100) break;
		}
	}
	

}
