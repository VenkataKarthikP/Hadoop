package com.cloudwick.hadoop.wordcount;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;


public class WordCountManager extends Mapper<LongWritable, Text, Text, IntWritable>{

	TaskID i = new TaskID();
	@SuppressWarnings("unchecked")
	protected void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		//System.out.println(key);
		
		//JobConf.get("mapred.task.id");
		System.out.println(context.getJobID());
		
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
