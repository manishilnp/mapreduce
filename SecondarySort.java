package com.manish.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {
		
		public static class sortMapper extends Mapper<Object, Text, CompositeKey, Text>{
			
			private CompositeKey comp = new CompositeKey();
			private Text va = new Text();
			@Override
			public void map(Object key, Text value, Context context) throws InterruptedException, IOException
			{
				String[] line = value.toString().split("\t");
				System.out.println(value+" "+line[1]);
				comp.state = line[0];
				comp.city = line[1];
				comp.total = Float.parseFloat(line[2]);	
				va.set(line[1]+":"+line[2]);
				context.write(comp, va);
			}
		}
		
		public static class sortReducer extends Reducer<CompositeKey, Text, Text, Text>{
			
			private Text ke= new Text();
			private Text val = new Text();
			@Override 
			public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
			{
				StringBuilder valuesInList = new StringBuilder();
				for(Text t: values)
				{
					valuesInList.append("(").append(t.toString()).append(")");
				}
				ke.set(key.state);
				val.set(valuesInList.toString());
				context.write(ke, val);
			}
		}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(SecondarySort.class);
			job.setMapperClass(sortMapper.class);
			job.setPartitionerClass(CustomPartitioner.class);
			job.setReducerClass(sortReducer.class);
			job.setMapOutputKeyClass(CompositeKey.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(3);
			job.setGroupingComparatorClass(GroupComparator.class);
			job.setSortComparatorClass(SortComparator.class);
			
			FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/sort/input"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/sort/output"));
			
			
			System.exit(job.waitForCompletion(true)?0:1);
		}

}
