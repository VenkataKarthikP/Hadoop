package com.cloudwick.hadoop.countpairs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountPairsReducer extends Reducer<TextPair, IntWritable, Text, IntWritable> {
	
	IntWritable count=new IntWritable(0);
	Text out = new Text("");
	int temp=0;
	protected void reduce(TextPair arg0, Iterable<IntWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println(arg0);
		Iterator<IntWritable> itr  = arg1.iterator();
		temp =0;
		//if(arg0.toString() != "")
		{
			while(itr.hasNext())
			{
				temp++;
				itr.next();
			}
			
			count.set(temp);
			out.set(arg0.toString());
			arg2.write(out,count);
		}
		
	}
}
