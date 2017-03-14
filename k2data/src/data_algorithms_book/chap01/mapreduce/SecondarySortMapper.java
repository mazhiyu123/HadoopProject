package data_algorithms_book.chap01.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper 
				extends Mapper<LongWritable, Text, DateTemperaturePair, Text>{
	
	private final Text theTemperature = new Text();
	private final DateTemperaturePair pair = new DateTemperaturePair();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");
		String yearMonth = tokens[0] + tokens[1];
		String day = tokens[2];
		int temperature = Integer.parseInt(tokens[3]);
		
		pair.setYearMonth(yearMonth);
		pair.setDay(day);
		pair.setTemperature(temperature);
		theTemperature.set(tokens[3]);
		
		context.write(pair, theTemperature);
	}
	
}
