package com.mapreduce.hw2.domain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * A group comparator to group the reduce tasks 
 * into one task. This is done by considering the station id
 * as key for grouping
 */
public class NaturalKeyGroupComparator extends WritableComparator{
	protected NaturalKeyGroupComparator() {
        super(Text.class, true);
    }
	
	@SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) {
        String k1 = ((Text) w1).toString();
        String k2 = ((Text) w2).toString();
        
        String k1stationId = k1.split("-")[0];
        String k2stationId = k2.split("-")[0];
        
        return k1stationId.compareTo(k2stationId);
    }
}
