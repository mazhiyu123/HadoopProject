package com.mzy.KmeansPreData;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansPreDataMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public final Text mapOutKey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] trafficInfo = value.toString().split(",");
		mapOutKey.set(trafficInfo[0]);
		StringBuilder mapOutValue = new StringBuilder();
		mapOutValue.append(trafficInfo[2]);
		mapOutValue.append(",");
		mapOutValue.append(trafficInfo[3]);
		mapOutValue.append(",");
		mapOutValue.append(trafficInfo[4]);
		
		context.write(mapOutKey, new Text(mapOutKey.toString()));
	}
}
