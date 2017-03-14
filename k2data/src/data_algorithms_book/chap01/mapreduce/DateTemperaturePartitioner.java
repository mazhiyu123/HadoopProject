package data_algorithms_book.chap01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text>{
	@Override
	public int getPartition(DateTemperaturePair pair,
							Text text,
							int numberOfPartitions) {
		
		return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
	}
}
