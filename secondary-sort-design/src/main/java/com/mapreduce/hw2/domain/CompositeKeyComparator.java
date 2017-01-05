package com.mapreduce.hw2.domain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * Composite key comparator to sort natural key and natural value
 */
public class CompositeKeyComparator extends WritableComparator{
	protected CompositeKeyComparator() {
        super(Text.class, true);
    } 
		
	/*
	 * Compare function to sort first based on natural keys 
	 * and then on natural value
	 */
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) {
        String k1 = ((Text)w1).toString();
        String k2 = ((Text)w2).toString();
        
        String k1stationId = k1.split("-")[0];
        String k2stationId = k2.split("-")[0];
        int result = k1stationId.compareTo(k2stationId);
        if(0 == result) {
        	int k1year = Integer.parseInt(k1.split("-")[1]);
        	int k2year = Integer.parseInt(k2.split("-")[1]);
        	// sorting in ascending order
            result =  (k1year-k2year);
        }
        return result;
    }
}
