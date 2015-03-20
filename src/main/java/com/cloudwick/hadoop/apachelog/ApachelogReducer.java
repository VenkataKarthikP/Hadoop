package com.cloudwick.hadoop.apachelog;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Venkata Karthik
 *
 */
public class ApachelogReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable out = new IntWritable();
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		Iterator<IntWritable> itr = arg1.iterator();
		int count = 0;
		while(itr.hasNext()){
			count ++;	
			itr.next();
		}
		out.set(count);
		arg2.write(arg0, out);
	}
		
}
