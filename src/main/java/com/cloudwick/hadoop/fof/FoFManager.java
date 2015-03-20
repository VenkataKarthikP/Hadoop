package com.cloudwick.hadoop.fof;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FoFManager  extends Mapper<LongWritable, Text, Text, Text>{
	
	
	@Override
	protected void map(LongWritable key, Text line,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(! line.toString().trim().equals(""))
		{
		
			//words[0] has node
			//friends will has all friends of words[0]
			
		String[] words = line.toString().split(" ");
		
		words[1] = words[1].replace('(', ' ');
		words[1] = words[1].replace(')', ' ');
		words[1] = words[1].trim();
		String[] friends = words[1].split(",");
		
		for(String str : friends)
		{
			System.out.println("In mapper" + words[0] + "   "+str+" f");
			context.write( new Text(words[0]), new Text(str+" f"));
		}
		
		for(String str : friends)
		{
			for(String str1 : friends)
			{
				if(! str.equals(str1)){
					System.out.println("In mapper" + str + "   "+str1+" s");
			context.write( new Text(str), new Text(str1+" s"));
				}
			}
		}
		}
	}

}
