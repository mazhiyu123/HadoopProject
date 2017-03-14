package data_algorithms_book.chap04.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinTransactionMapper 
						extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings>{
	
	PairOfStrings outKey = new PairOfStrings();
	PairOfStrings outValue = new PairOfStrings();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString(), "\t");
		
			outKey.set(tokens[2], "2");
			outValue.set("P", tokens[1]);
			context.write(outKey, outValue);
	}
}
