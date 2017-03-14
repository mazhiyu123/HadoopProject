package data_algorithms_book.chap05.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyCombiner 
				extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable>{
	
	@Override
	protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable num : values){
			sum += num.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
