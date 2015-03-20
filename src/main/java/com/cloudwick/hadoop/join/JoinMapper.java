package com.cloudwick.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, CustomInput, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		System.out.println("Im mapper Class");
		String line = value.toString();
		String[] line_arr = line.split(" ");
		
		if(line_arr.length == 2)
		{
			context.write(new CustomInput(line_arr[2], 0), new Text( line));
		}
		else
		{
			context.write(new CustomInput(line_arr[0],1), new Text(line));
		}
		
	}
	
}
