package com.mapreduce.hw2.domain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.mapreduce.hw2.valueobjects.TempValueWritable;
import com.mapreduce.hw2.valueobjects.TemperatureValueWritable;

/*
 * Partitioner to partition based on the 
 * keys - station id to route all the same station ids to
 * the same reducer
 */
public class KeyPartitioner extends Partitioner<Text, TempValueWritable> {

	@Override
	
	/*
	 * Fetch the hash code of the key and determine the reducer
	 */
	public int getPartition(Text key, TempValueWritable value, int numPartitions) {
		int hash = key.toString().split("-")[0].hashCode();
        int partition = hash % numPartitions;
        return Math.abs(partition);
	}

}
