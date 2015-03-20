package com.cloudwick.hadoop.countpairs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountPairsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

	Logger log = LoggerFactory.getLogger(CountPairsMapper.class);
	IntWritable count = new IntWritable(1);
	TextPair tp ;
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if( value != null){
		String arg = value.toString();
		String[] args = arg.split(" ");
		
		tp = new TextPair(args[0],args[1]);
		
		if(args.length == 2)
			context.write(tp,count);
		else
			log.warn("Doesnot Contain a valid line");
		}
		else
			log.warn("Input value to mapper is null");
		
	}
	
}
