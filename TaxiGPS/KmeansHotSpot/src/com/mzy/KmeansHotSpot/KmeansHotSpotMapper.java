package com.mzy.KmeansHotSpot;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansHotSpotMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	IntWritable outMapValue = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] recordInfo = value.toString().split("\t");
		IntWritable outMapKey  = new  IntWritable(Integer.parseInt(recordInfo[0]));
		context.write(outMapKey, outMapValue);
	}
	
}
