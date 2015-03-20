package com.cloudwick.hadoop.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountManager {

	public class map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@SuppressWarnings("unchecked")
		protected  map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(line.toString().trim().equals(""));
			{
			
			String[] words = line.toString().split(" ");
			
			
			for (String word : words) {
				word = word.replace('"', ' ');
				word =  word.replace('\'',' ');
				word = word.replace('.',' ');
				word = word.replace('!',' ');
				word = word.replace(',',' ');
				word = word.replace('?',' ');
				word = word.trim();		
				if(word.equals(""))
					continue;
				else
				context.write(new Text(word), new IntWritable(1));
				
			}
			}
		}
		
	} 
	
	
	public class reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		@SuppressWarnings("unchecked")
		protected reduce(Text arg0, Iterable<IntWritable> count, Context arg2)
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
}
