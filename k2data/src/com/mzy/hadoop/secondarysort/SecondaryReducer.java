package com.mzy.hadoop.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondaryReducer extends Reducer<Text,IntWritable,NullWritable,Text> {
	protected void reduce(Text key,java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for ( Text value : values){
			context.write(NullWritable.get(), value);
		}
	}
}
