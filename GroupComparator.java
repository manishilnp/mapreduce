package com.manish.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{
	
	public GroupComparator()
	{
		super(CompositeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		CompositeKey compKey1 = (CompositeKey) w1;
		CompositeKey compKey2 = (CompositeKey) w2;
		
		return compKey1.state.compareTo(compKey2.state);
	}
}
