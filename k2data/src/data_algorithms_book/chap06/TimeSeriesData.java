package data_algorithms_book.chap06;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import data_algorithms_book.util.DateUtil;

public class TimeSeriesData implements Writable, Comparable<TimeSeriesData>{
	
	private long timestamp;
	private double value;
	
	public static TimeSeriesData copy(TimeSeriesData tsd) {
		return new TimeSeriesData(tsd.timestamp, tsd.value);
	}
	
	public TimeSeriesData(long timestamp, double value) {
		set(timestamp, value);
	}
	
	public TimeSeriesData(){}
	
	public void set(long timestamp, double value) {
		this.timestamp = timestamp;
		this.value = value;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public double getValue() {
		return this.value;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		this.value = in.readDouble();
	}
	
	public static TimeSeriesData read(DataInput in) throws IOException {
		TimeSeriesData tsData = new TimeSeriesData();
		tsData.readFields(in);
		return tsData;
	}
	
	public String getDate() {
		return DateUtil.getDateAsString(this.timestamp);
	}
	
	public TimeSeriesData clone() {
		return new TimeSeriesData(this.timestamp, this.value);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeDouble(value);
	}
	
	@Override
	public int compareTo(TimeSeriesData data) {
		if (this.timestamp > data.timestamp) {
			return 1;
		} else if(this.timestamp < data.timestamp) {
			return -1;
		} else {
			return 0;
		}
	}
	
	public String toString() {
	       return "("+timestamp+","+value+")";
	    }
}
