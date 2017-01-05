package com.pagerank.domain.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.pagerank.parser.Bz2WikiParser;
import com.pagerank.valueobjects.PageNode;

public class PageRankMapperParserV1 extends Mapper<LongWritable, Text, Text, PageNode>{
	
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		int delimLoc = value.toString().indexOf(':');
		String pageName = value.toString().substring(0, delimLoc);
		Set<String> adjSet = Bz2WikiParser.parser(value.toString());
		if(adjSet!=null){
			List<String> adjList = new ArrayList<>(adjSet);
			PageNode pgNode = new PageNode(0.0,adjList.stream().toArray(String[]::new), true);
			output.write(new Text(pageName), pgNode);
		}
	}	
}
