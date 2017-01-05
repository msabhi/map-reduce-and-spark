package com.pagerank.domain.topk;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;



public class TopKKeyPartitioner extends Partitioner<PageEntries, NullWritable>  {
	
	@Override
	
	/*
	 * Fetch the hash code of the key and determine the reducer
	 * which is always 1 in our case
	 */
	public int getPartition(PageEntries key, NullWritable value, int numPartitions) {
		int hash = key.pageName.hashCode();
        int partition = hash % numPartitions;
        return Math.abs(partition);
	}

}
