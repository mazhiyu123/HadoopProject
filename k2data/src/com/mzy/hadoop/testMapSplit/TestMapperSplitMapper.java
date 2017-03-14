package com.mzy.hadoop.testMapSplit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TestMapperSplitMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split(",");
		
		 Text mapout = new Text();
		 Text mapvalue = new Text();
		
		mapout.set(value.toString().split(",")[4] + lines[3]);
		mapvalue.set(lines[0] + lines[1]);
		
		context.write(mapout, mapvalue);
	}
	
}
