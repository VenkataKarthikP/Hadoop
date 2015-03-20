package com.cloudwick.hadoop.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<CustomInput, Text, Text, Text>{

	@Override
	protected void reduce(CustomInput arg0, Iterable<Text> arg1,Context arg2) throws IOException,InterruptedException {
		// TODO Auto-generated method stub
			
			System.out.println("In Reducer Class");
			Iterator<Text> itr = arg1.iterator();
			
			String dname = null;
			Boolean first = true;
			
			while(itr.hasNext())
			{
				if(first){
				dname =  new String(itr.next().toString().split(" ")[1]);
				first = false;
				}
				else{
					
					String[] line = itr.next().toString().split(" ");
					
					arg2.write(new Text(line[0] + line[1] + dname), new Text(" "));
					
				}
				
				
				
			}
		
		
		
	}
	
	
}
