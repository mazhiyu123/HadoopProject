package data_algorithms_book.chap04.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinUserMapper 
				extends Mapper<LongWritable, Text, PairOfStrings,PairOfStrings>{

	PairOfStrings outputKey = new PairOfStrings();
	PairOfStrings outputValue = new PairOfStrings();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString(), "\t");
		if (tokens.length == 2) {
			outputKey.set(tokens[0], "1");
			outputValue.set("L", tokens[1]);
			context.write(outputKey, outputValue);
		}
	}
}
