package com.mzy.hadoop.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondaryMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		context.write(value, NullWritable.get());
	}
}
