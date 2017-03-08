package com.mzy.KmeansHotSpot;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KmeansHotSpotDriver  extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new KmeansHotSpotDriver(), args);
	}
	
	public int run(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		
		job.setJobName("KmeansHotSpot");
		job.setJarByClass(KmeansHotSpotDriver.class);
		
		job.setMapperClass(KmeansHotSpotMapper.class);
		job.setReducerClass(KmeansHotSpotReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(KmeansHotSpotCombiner.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
