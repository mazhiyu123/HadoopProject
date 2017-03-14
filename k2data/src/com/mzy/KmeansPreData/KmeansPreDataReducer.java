package com.mzy.KmeansPreData;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansPreDataReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	public int row = 0;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Boolean hasPassenger = false;
		StringBuilder builder = new StringBuilder();
		
		for (Text value : values) {
			String[] lines = values.toString().split(",");
			
			if (!hasPassenger && lines[0].equals("1")) {
				// 上车地点
				hasPassenger = true;
				builder.append(key.toString());
				builder.append(",");
				builder.append(lines[3]);
				builder.append(",");
				builder.append(lines[4]);
				
				context.write(key, NullWritable.get());
			}
		}
	}
	
}
