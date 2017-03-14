package com.mzy.hadoop.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class KeyPartioner extends HashPartitioner<Text, NullWritable> {
	@Override
	public int getPartition(Text key, NullWritable value, int numReduceTasks){
		return (key.toString().split(" ")[0].hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}
