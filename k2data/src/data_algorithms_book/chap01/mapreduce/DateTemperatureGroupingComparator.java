package data_algorithms_book.chap01.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator {
	public DateTemperatureGroupingComparator(){
		super(DateTemperaturePair.class, true);
	}
	
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		DateTemperaturePair pair = (DateTemperaturePair) wc1;
		DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
		return pair.getYearMonth().compareTo(pair2.getYearMonth());
	}
}
