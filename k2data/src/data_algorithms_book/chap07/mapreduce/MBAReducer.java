package data_algorithms_book.chap07.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
	   @Override
	   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	      int sum = 0; // total items paired
	      for (IntWritable value : values) {
	         sum += value.get();
	      }
	      context.write(key, new IntWritable(sum));
	   }
	}
