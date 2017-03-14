package com.mzy.KmeansSecondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TaxiIDKeyPartitioner extends Partitioner<TaxiIDDateKey, Text> {
	
	@Override
	public int getPartition(TaxiIDDateKey key , Text value, int numOfReducer) {
		return Math.abs((key.getTaxiID().hashCode()) % numOfReducer);
	}
	
	 
}
