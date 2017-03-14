package com.mzy.hadoop.totalsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalSort {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		// 分区文件的路径
		Path partitionPath = new Path(args[2]);
		
		int numReduceTask = Integer.parseInt(args[3]);
		
		// 第一个参数 会被选中的概率 第二个参数选取的样本数  第三个参数是读取的最大InputSplit的个数
		RandomSampler<Text,Text> sampler = new InputSampler.RandomSampler<Text,Text>(0.1, 1000,10);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"totalSort");
		
		job.setJarByClass(TotalSort.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(numReduceTask);
		
		// 设置Partition类
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// 写入分区文件
		InputSampler.writePartitionFile(job, sampler);
		
		System.exit(job.waitForCompletion(true) ? 1 :0);
	}

}
