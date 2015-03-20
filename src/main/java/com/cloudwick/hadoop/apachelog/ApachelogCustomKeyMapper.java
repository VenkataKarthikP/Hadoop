package com.cloudwick.hadoop.apachelog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApachelogCustomKeyMapper extends Mapper<LongWritable,Text,CustomKey,Text>{

	Logger logger = LoggerFactory.getLogger(ApachelogMapper.class);
	public static final Pattern apacheLogPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[(.*)\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
	CustomKey ck ;
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Matcher matcher = apacheLogPattern.matcher(value.toString());
		
		if(matcher.find()){
			
			ck = new CustomKey(matcher.group(1),matcher.group(5),matcher.group(4));
			logger.info(matcher.group(0));
			logger.info(matcher.group(6));
			logger.info(matcher.group(4));
			context.write(ck,value);
		}
	}
}
