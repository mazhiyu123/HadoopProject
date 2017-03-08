package com.mzy.KmeansSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaxiIDKeyGroupingComparatort extends WritableComparator{
	
	protected TaxiIDKeyGroupingComparatort() {
		super(TaxiIDDateKey.class,true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		TaxiIDDateKey record1 = (TaxiIDDateKey) wc1;
		TaxiIDDateKey record2 = (TaxiIDDateKey) wc2;
		
		if (record1.getTaxiID().compareTo(record1.getTaxiID()) != 0) {
			return record1.getTaxiID().compareTo(record1.getTaxiID());
		} else {
			return 0;
		}
	}
	
}
