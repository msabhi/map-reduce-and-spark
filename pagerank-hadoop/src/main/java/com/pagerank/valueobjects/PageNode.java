package com.pagerank.valueobjects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.Utils;


public class PageNode implements Writable{

	private double pageRank;
	private boolean isPrimaryOrSinkNode; 
	private String[] adjacencyList;
	
	public PageNode(){
		adjacencyList = new String[0];
	}
	
	public PageNode(double pr, String[] adjList, boolean isPrimaryOrSinkNode){
		pageRank = pr;
		adjacencyList = adjList;
		this.isPrimaryOrSinkNode = isPrimaryOrSinkNode;
	}

	public double getPageRank(){
		return this.pageRank;
	}
	
	public void setPageRank(double pr){
		this.pageRank = pr;
	}
	
	public String[] getAdjacentList(){
		return this.adjacencyList;
	}
	
	public boolean getIsPrimaryOrSinkNode(){
		return this.isPrimaryOrSinkNode;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		Utils.writeString(out, pageRank+"");
		Utils.writeString(out, isPrimaryOrSinkNode+"");
		WritableUtils.writeStringArray(out, adjacencyList);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank = Double.parseDouble(Utils.readString(in));
		isPrimaryOrSinkNode = Boolean.parseBoolean(Utils.readString(in));
		adjacencyList = WritableUtils.readStringArray(in);
	}

	public String toString(){
		return this.pageRank+"~~~"+Arrays.asList(adjacencyList).stream().collect(Collectors.joining(","));
	}

}
