package data_algorithms_book.chap06.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import data_algorithms_book.chap06.TimeSeriesData;
import data_algorithms_book.util.DateUtil;

public class SortInMemory_MovingAverageReducer 
					extends Reducer<Text, TimeSeriesData, Text, Text>{
	
	int windowSize = 5;
	
	public void setup(Context context) {
		this.windowSize = context.getConfiguration().getInt("moving.average.window.size", 5);
		System.out.println("setup(): key = " + windowSize);
	}
	
	@Override
	public void reduce(Text key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {
		
		System.out.println("reduce():key =  " + key.toString());
		
		List<TimeSeriesData> timeseries = new ArrayList<TimeSeriesData>();
		for(TimeSeriesData data : values) {
			TimeSeriesData copy =  TimeSeriesData.copy(data);
			timeseries.add(copy);
		}
		
		Collections.sort(timeseries);
		System.out.println("reduce(): timeseries="+timeseries.toString());
		
		double sum = 0.0;
		for (int i = 0; i < windowSize; i++) {
			sum += timeseries.get(i).getValue();
		}
		
		Text outputValue = new Text();
		for (int i = windowSize - 1; i < timeseries.size(); i++) {
			sum += timeseries.get(i).getValue();
			double movingAverage = sum/windowSize;
			long timestamp = timeseries.get(i).getTimestamp();
			outputValue.set(DateUtil.getDateAsString(timestamp) + "," + movingAverage);
			context.write(key, outputValue);
			
			sum -= timeseries.get(i - windowSize + 1).getValue();
		}
	}
}
