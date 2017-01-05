package com.pagerank.domain.topk;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.pagerank.valueobjects.PageEntries;

/*
 * A group comparator to group the reduce tasks 
 * into one task. This is done by considering 
 * everything as single record
 */
public class NaturalKeyGroupComparator extends WritableComparator{
	protected NaturalKeyGroupComparator() {
        super(PageEntries.class, true);
    }
	
	@SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) {
        return 0;
    }
}
