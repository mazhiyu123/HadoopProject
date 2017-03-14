package data_algorithms_book.chap05.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer 
					extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable>{
	
	private double totalCount = 0;
	private final DoubleWritable relative = new DoubleWritable();
	private String currentWord = "NOT_DEFINED";
	
	@Override
	protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		if (key.getNeighbor().equals("*")) {
			if (key.getWord().equals(currentWord)) {
				totalCount += totalCount + getTotalCount(values);
			} else {
				currentWord = key.getWord();
				totalCount = getTotalCount(values);
			}
		} else {
			int count = getTotalCount(values);
			relative.set((double) count / totalCount);
			context.write(key, relative);
		}
		
	}
	 private int getTotalCount(Iterable<IntWritable> values) {
	        int sum = 0;
	        for (IntWritable value : values) {
	            sum += value.get();
	        }
	        return sum;
	    }
	
}
