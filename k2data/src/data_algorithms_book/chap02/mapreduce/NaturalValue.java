package data_algorithms_book.chap02.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import data_algorithms_book.util.DateUtil;

public class NaturalValue implements Writable, Comparable<NaturalValue>{
	private long timestamp;
	private double price;
	
	public static NaturalValue copy(NaturalValue value) {
		return new NaturalValue(value.timestamp, value.price);
	}
	
	public NaturalValue(long timestamp, double price) {
		set(timestamp, price);
	}
	
	public void set(long timestamp, double price) {
		this.timestamp = timestamp;
		this.price = price;
	}
	
	public NaturalValue(){}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		this.price = in.readDouble();
	}
	
	public static NaturalValue read(DataInput in) throws IOException {
		NaturalValue value = new NaturalValue();
		value.readFields(in);
		return value;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public double getPrice() {
		return this.price;
	}
	
	public String getDate() {
		return DateUtil.getDateAsString(this.timestamp);
	}
	
	public NaturalValue clone() {
		return new NaturalValue(timestamp, price);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeDouble(price);
	}
	
	@Override
	public int compareTo(NaturalValue data) {
		if (this.timestamp < data.timestamp) {
			return -1;
		} else if (this.timestamp > data.timestamp) {
			return 1;
		} else {
			return 0;
		}
	}
}
