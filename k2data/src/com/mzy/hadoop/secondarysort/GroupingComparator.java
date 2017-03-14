package com.mzy.hadoop.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
	protected GroupingComparator(){
		super(Text.class,true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		
		if (Integer.parseInt(w1.toString().split(" ")[0]) == Integer.parseInt(w2.toString().split(" ")[0])){
			return 0;
		} else if (Integer.parseInt(w1.toString().split(" ")[0]) > Integer.parseInt(w2.toString().split(" ")[0])){
			return 1;
		} else if (Integer.parseInt(w1.toString().split(" ")[0]) < Integer.parseInt(w2.toString().split(" ")[0])){
			return -1;
		}
		return 0;
	}
}
