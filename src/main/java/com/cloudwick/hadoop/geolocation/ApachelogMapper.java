package com.cloudwick.hadoop.geolocation;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Venkata Karthik
 *
 */
public class ApachelogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Logger logger = LoggerFactory.getLogger(ApachelogMapper.class);
	public static final Pattern apacheLogPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[(.*)\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
	private Text ipAddress = new Text();
	GeoLocation gl;
	private static final IntWritable COUNT = new IntWritable(1);
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		logger.info("In mapper Class");
		 gl = new GeoLocation("C:\\Users\\Venkata Karthik\\Downloads\\GeoLite2-City.mmdb");
		
	}
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Matcher matcher = apacheLogPattern.matcher(value.toString());
		if(matcher.find()){
			
		ipAddress.set(gl.getLocation(matcher.group(1)));
		context.write(ipAddress,COUNT);
		
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

}
