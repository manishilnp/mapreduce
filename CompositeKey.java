package com.manish.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{

	String state;
	String city;
	float total;
	public CompositeKey()
	{
		
	}
	
	public CompositeKey(String s, String c, float t)
	{
		super();
		this.state = s;
		this.city = c;
		this.total = t;
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(state);
		out.writeUTF(city);
		out.writeFloat(total);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		state = in.readUTF();
		city = in.readUTF();
		total = in.readFloat();
	}
	
	public String toString()
	{
		return state+"\t"+city+"\t"+total;
	}
	
	@Override
	public int compareTo(CompositeKey ob)
	{
		int result = state.compareTo(ob.state);
		if(result != 0)
		{
			return result;
		}
		else
		{
			if(city.compareTo(ob.city) != 0)
			{
				return city.compareTo(ob.city);
			}
			else
			{
				return Float.compare(total, ob.total);
			}
		}
	}
}
