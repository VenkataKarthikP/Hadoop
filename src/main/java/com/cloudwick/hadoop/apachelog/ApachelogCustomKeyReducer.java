package com.cloudwick.hadoop.apachelog;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ApachelogCustomKeyReducer extends Reducer<CustomKey, Text, Text, NullWritable>{

	protected void reduce(CustomKey arg0, Iterable<Text> arg1,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Iterator<Text> itr = arg1.iterator();
		
		context.write(itr.next(), NullWritable.get());
	}
	
}
