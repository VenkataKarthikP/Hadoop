package com.cloudwick.hadoop.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerExp extends Reducer<Text, IntWritable, Text, IntWritable>{

	@SuppressWarnings("unchecked")
	protected void reduce(Text arg0, Iterable<IntWritable> count, Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		Iterator<IntWritable> itr = count.iterator();
		int sum=0;
		
		
		while(itr.hasNext())
		{
			sum++;
			itr.next();
		}
		
		arg2.write(arg0, new IntWritable(sum));
		
	}
	
}

