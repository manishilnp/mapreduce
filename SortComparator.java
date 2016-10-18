package com.manish.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{
	
	public SortComparator()
	{
		super(CompositeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		CompositeKey compKey1 = (CompositeKey) w1;
		CompositeKey compKey2 = (CompositeKey) w2;
		
		int result = compKey1.state.compareTo(compKey2.state);
		if(result != 0)
		{
			return result;
		}
		else
		{
			if(compKey1.city.compareTo(compKey2.city) != 0)
			{
				return compKey1.city.compareTo(compKey2.city);
			}
			else
			{
				return Float.compare(compKey1.total, compKey2.total);
			}
		}
	}
}
