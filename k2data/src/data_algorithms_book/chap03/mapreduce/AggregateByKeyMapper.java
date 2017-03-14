package data_algorithms_book.chap03.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateByKeyMapper extends Mapper<Object, Text, Text , IntWritable>{
	private Text K2 = new Text();
	private IntWritable V2 = new IntWritable();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String valueAsString = value.toString().trim();
		String[] tokens = valueAsString.split(",");
		if (tokens.length != 2) {
			return ;
		}
		
		String url = tokens[0];
		int frequency = Integer.parseInt(tokens[1]);
		K2.set(url);
		V2.set(frequency);
		context.write(K2, V2);
	}
}
