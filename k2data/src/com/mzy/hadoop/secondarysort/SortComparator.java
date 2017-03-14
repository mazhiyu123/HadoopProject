package com.mzy.hadoop.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class SortComparator extends WritableComparator {
	
	protected SortComparator(){
		super(Text.class,true);
	}
	
	@Override
	public int compare(WritableComparable key1, WritableComparable key2){
		if (Integer.parseInt(key1.toString().split(" ")[0]) == Integer.parseInt(key2.toString().split(" ")[1])){
			if (Integer.parseInt(key1.toString().split(" ")[1]) > Integer.parseInt(key2.toString().split(" ")[1])){
				return 1;
			} else if (Integer.parseInt(key1.toString().split(" ")[1]) < Integer.parseInt(key2.toString().split(" ")[1])){
				return -1;
			} else {
				return 0;
			}
		} else {
			if (Integer.parseInt(key1.toString().split(" ")[0]) > Integer.parseInt(key2.toString().split(" ")[0])){
				return 1;
			} else if (Integer.parseInt(key1.toString().split(" ")[0]) < Integer.parseInt(key2.toString().split(" ")[0])){
				return -1;
			}
		}
		return 0;
	}
	
}
