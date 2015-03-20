package com.cloudwick.hadoop.countpairs;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class Driver extends Configured implements Tool {

	Logger log = LoggerFactory.getLogger(Driver.class);
	
    @SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            log.error("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
            return -1;
        }
      
        Job job = new Job(getConf());
        job.setJarByClass(Driver.class);
        job.setJobName(this.getClass().getName());

        Configuration conf = new Configuration();
        job.getConfiguration().set("fs.file.impl",
				"com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
      
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CountPairsMapper.class);
        job.setReducerClass(CountPairsReducer.class);
        job.setSortComparatorClass(CustomKeyComparator.class);
       
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static boolean deleteDir(File dir) 
    { 
    	
      if (dir.isDirectory()) 
    { 
      String[] children = dir.list(); 
      for (int i=0; i<children.length; i++)
      { 
        boolean success = deleteDir(new File(dir, children[i])); 
        if (!success) 
        {  
          return false; 
        } 
      } 
    }  
      // The directory is now empty or this is a file so delete it 
      return dir.delete(); 
    }
    public static void main(String[] args) throws Exception {
    
    	Logger log = LoggerFactory.getLogger(Driver.class);
    	log.info("Staring the Execution");
    	File output  = new File(args[1]);
    	deleteDir(output);
    	
    	int exitCode = ToolRunner.run(new Driver(), args);
    	log.info("Completed Execution");
        System.exit(exitCode);
    }
}