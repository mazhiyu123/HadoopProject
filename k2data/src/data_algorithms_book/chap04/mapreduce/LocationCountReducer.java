package data_algorithms_book.chap04.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocationCountReducer 
					extends Reducer<Text, Text, Text, LongWritable>{
	
	Set<String> set = new HashSet<String>();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text text : values) {
			set.add(text.toString());
		}
		context.write(key, new LongWritable(set.size()));
	}
	
}
