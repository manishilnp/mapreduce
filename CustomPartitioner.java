package com.manish.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CompositeKey, Text>{

	@Override
	public int getPartition(CompositeKey key, Text val, int numReducer)
	{
		String state = key.state;
		return key.state.hashCode()%numReducer;
		
		/*if(Character.compare(state.charAt(0), 'm')<0)
		{
			return 1;
		}
		else if(Character.compare(state.charAt(0), 't')<0)
		{
			return 2;
		}
		else
			return 3;*/
	}
}
