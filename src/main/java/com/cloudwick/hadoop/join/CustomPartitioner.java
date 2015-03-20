package com.cloudwick.hadoop.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class CustomPartitioner  extends Partitioner<CustomInput, Text>{

	
	Text newkey = new Text();
	HashPartitioner<Text, Text> hash = new HashPartitioner<Text, Text>();
	@Override
	public int getPartition(CustomInput arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		
		newkey.set(arg0.did);
		
		return hash.getPartition(newkey,arg1,arg2);
				//return 0;
	}
	
}
