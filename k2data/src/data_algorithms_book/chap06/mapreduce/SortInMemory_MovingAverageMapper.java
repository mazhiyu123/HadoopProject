package data_algorithms_book.chap06.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import data_algorithms_book.chap06.TimeSeriesData;
import data_algorithms_book.util.DateUtil;

public class SortInMemory_MovingAverageMapper 
				extends Mapper<LongWritable, Text, Text, TimeSeriesData>{
	
		private final Text reduceKey = new Text();
		private final TimeSeriesData reduceValue = new TimeSeriesData();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			if ((record == null) || (record.length() == 0)) {
				return ;
			}
			String[] tokens = StringUtils.split(record.trim(), ",");
			if (tokens.length == 3) {
				Date date = DateUtil.getDate(tokens[1]);
				if (date == null) {
					return ;
				}
				reduceKey.set(tokens[0]);
				reduceValue.set(date.getTime(), Double.parseDouble(tokens[2]));
				context.write(reduceKey, reduceValue);
			} else {
				
			}
		}
}
