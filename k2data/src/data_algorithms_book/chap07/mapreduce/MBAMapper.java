 package data_algorithms_book.chap07.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import data_algorithms_book.util.Combination;

public class MBAMapper  extends Mapper<LongWritable, Text, Text, IntWritable>{
	public static final Logger THE_LOGGER = Logger.getLogger(MBAMapper.class);
	
	public static final int DEFAULT_NUMBER_OF_PAIRS = 2;
	
	private static final Text reduceKey = new Text();
	private static final IntWritable NUMBER_ONE = new IntWritable(1);
	
	int numberOfPairs;
	
	@Override
	protected void setup(Context context) {
		this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
		THE_LOGGER.info("setup() numberOfPairs = " + numberOfPairs);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		List<String> items = convertItemsToList(line);
		if ((items == null) || (items.isEmpty())) {
			return ;
		}
		generateMapperOutput(numberOfPairs, items, context);
	}
	
	private static List<String> convertItemsToList(String line) {
		if ((line == null) || (line.length() == 0)) {
			return null;
		}
		String[] tokens = StringUtils.split(line, ",");
		if ((tokens == null) || (tokens.length == 0)) {
			return null;
		}
		List<String> items = new ArrayList<String>();
		for (String token : tokens) {
			if (token != null ) {
				items.add(token);
			}
		}
		return items;
	}
	
	private void generateMapperOutput(int numberOfPairs, List<String> items, Context context) throws IOException, InterruptedException {
		List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
		
		for (List<String> itemList : sortedCombinations) {
			reduceKey.set(itemList.toString());
			context.write(reduceKey, NUMBER_ONE);
		}
	}
}
