package com.mzy.KmeansSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TaxiIDDateKey implements WritableComparable<TaxiIDDateKey>{
	private Integer taxiID;
	private String date;
	
	public TaxiIDDateKey(Integer taxiID, String date) {
		this.taxiID = taxiID;
		this.date = date;
	}
	
	public TaxiIDDateKey() {}
	
	
	public Integer getTaxiID() {
		return taxiID;
	}

	public void setTaxiID(Integer taxiID) {
		this.taxiID = taxiID;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public void readFields(DataInput in) throws IOException  {
		this.taxiID = in.readInt();
		this.date = in.readUTF(); // ?
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(taxiID);
		out.writeUTF(date);
	}
	
	@Override
	public int compareTo(TaxiIDDateKey other) {
		
		if (this.taxiID.compareTo(other.taxiID) != 0) {
			return this.taxiID.compareTo(other.taxiID);
		} else {
			return this.date.compareTo(other.date);
		}
	}

	@Override
	public String toString() {
		return taxiID + "," + date + ",";
	}
	

	
}
