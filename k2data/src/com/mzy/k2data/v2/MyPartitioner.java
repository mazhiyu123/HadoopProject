package com.mzy.k2data.v2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, Text> {
	public int getPartition(Text key, Text value, int numPartition ){
		if("normal".equals(key)){
			return 0%numPartition;
		}else {
			return 1%numPartition;
		}
			
	}
}
