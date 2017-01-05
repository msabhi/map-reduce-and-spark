package com.pagerank.domain.topk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.file.tfile.Utils;


public class PageEntries implements Comparable<PageEntries>, WritableComparable<PageEntries>, Writable{
	
	public PageEntries(){
		
	}
	public PageEntries(String pgName, double pgRank){
		this.pageName = pgName;
		this.pageRank = pgRank;
	}
	public String pageName;
	public double pageRank;

	@Override
	public int compareTo(PageEntries o) {
		return Double.compare(o.pageRank, this.pageRank);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Utils.writeString(out, pageName);
		Utils.writeString(out, pageRank+"");
		
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.pageName = Utils.readString(in);
		this.pageRank = Double.parseDouble(Utils.readString(in));
		
	}
}
