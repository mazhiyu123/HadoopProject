package data_algorithms_book.chap02.mapreduce;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import data_algorithms_book.util.DateUtil;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue>{
	private final CompositeKey reduceKey = new CompositeKey();
	private final NaturalValue reduceValue = new NaturalValue();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString().trim(), ",");
		if (tokens.length == 3) {
			Date date = DateUtil.getDate(tokens[1]);
			if (date == null) {
				return ;
			}
			long timestamp = date.getTime();
			reduceKey.set(tokens[0], timestamp);
			reduceValue.set(timestamp, Double.parseDouble(tokens[2]));
			context.write(reduceKey, reduceValue);
		}
	}
}
