package data_algorithms_book.chap08.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static final Text REDUCER_KEY = new Text();
	private static final Text REDUCER_VALUE = new Text();
	
	static String getFriends(String[] tokens) {
		if (tokens.length == 2) {
			return "";
		}
		StringBuilder builder = new StringBuilder();
		for (int i = 1; i < tokens.length; i++) {
			builder.append(tokens[i]);
			if (i < (tokens.length - 1)) {
				builder.append(",");
			}
		}
		return builder.toString();
	}
	
	static String builderSortedKey(String person, String friend) {
		long p = Long.parseLong(person);
		long f = Long.parseLong(friend);
		if (p < f) {
			return person + "," + friend;
		} else {
			return friend + "," + friend;
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString(), ",");
		String frieds = getFriends(tokens);
		REDUCER_VALUE.set(frieds);
		
		String person = tokens[0];
		for (int i = 1; i < tokens.length; i++) {
			REDUCER_KEY.set(builderSortedKey(person, tokens[i]));
			context.write(REDUCER_KEY, REDUCER_VALUE);
		}
	}
}
