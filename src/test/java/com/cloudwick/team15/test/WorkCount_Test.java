package com.cloudwick.team15.test;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.cloudwick.hadoop.wordcount.ReducerExp;
import com.cloudwick.hadoop.wordcount.WordCountManager;

public class WorkCount_Test {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    
    @Before
    public void setup() {
        WordCountManager mapper = new WordCountManager();
        ReducerExp reducer = new ReducerExp();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }
    
    @Test
    public void testMapper()  throws IOException{
        mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapDriver.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver.withOutput(new Text("dog"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("cat"), values);
        reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}