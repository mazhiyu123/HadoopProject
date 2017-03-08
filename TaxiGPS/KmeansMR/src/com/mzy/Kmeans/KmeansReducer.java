package com.mzy.Kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<IntWritable, Text, Text, NullWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int centerIndexNum = 0;
		Double[] coorSum = new Double[]{0.0, 0.0}; 
		for (Text value : values) {
			String[] coorInfo = value.toString().split(",");
			for(int i = 0; i < coorInfo.length; i++) {
				coorSum[i] += Double.parseDouble(coorInfo[i]);
			}
			centerIndexNum++;
		}
		
		StringBuilder outKey = new StringBuilder(key.toString());
		for(int i = 0; i < coorSum.length; i++) {
			coorSum[i] /= centerIndexNum;
			outKey.append(",");
			outKey.append(String.valueOf(coorSum[i]));
		}
		
		context.write(new Text(outKey.toString()), NullWritable.get());
		
	}
	
}
