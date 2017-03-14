package data_algorithms_book.chap01.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text>{
	
	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		StringBuilder builder = new StringBuilder();
		for (Text value : values) {
			builder.append(value);
			builder.append(",");
		}
		
		context.write(new Text(key.getYearMonth()), new Text(builder.toString()));
	}
}
