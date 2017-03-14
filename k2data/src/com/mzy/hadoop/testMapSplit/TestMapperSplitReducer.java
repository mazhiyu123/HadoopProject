package com.mzy.hadoop.testMapSplit;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TestMapperSplitReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*for(Text value : values) {
			context.write(key, value);
		}*/
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()) {
			context.write(key, itr.next());
		}
	}
}
