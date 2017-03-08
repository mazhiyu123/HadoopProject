package com.mzy.KmeansSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansSecondarySortReducer extends Reducer<TaxiIDDateKey, Text, Text, NullWritable>{
	
	int row = 0;
	
	protected void reduce(TaxiIDDateKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
	String preState = "0";
	String currentState = "0";
		
	/*for (Text value : values) {
		
		context.write(new Text(key.toString() + value.toString()), NullWritable.get());
	}*/
	// context.write(new Text(key.toString()), new Text(values.toString()));
	
	for (Text value : values) {
		String[] info = value.toString().split(",");
		
		currentState = info[0]; 
		if ((preState.compareTo("0") == 0) && (currentState.compareTo("1") == 0)) {
			StringBuilder keyBuilder = new StringBuilder();
			keyBuilder.append(String.valueOf(++row));
			keyBuilder.append(",");
			keyBuilder.append(info[1]);
			keyBuilder.append(",");
			keyBuilder.append(info[2]);
			//keyBuilder.append(",");
			//keyBuilder.append(key.toString());
			
			preState = "1";
			context.write(new Text(keyBuilder.toString()), NullWritable.get());
		}
		
		if ((preState.compareTo("1") == 0) && (currentState.compareTo("0") == 0)) {
			preState = "0";
		}
		
	}
	}
}
