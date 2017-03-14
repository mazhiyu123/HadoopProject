package com.mzy.KmeansSecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KmeansSecondarySortDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new Configuration(), new KmeansSecondarySortDriver(), args);
	}
	
	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//String inputPath = "hdfs://ns1/practice/Kmeans/inputData/train_test.txt";
		//String outputPath = "hdfs://ns1/practice/Kmeans/outputData/";
		
		Configuration conf = getConf();
		//conf.set("mapred.ifile.buffer.reset.size.mb", "256");
		
		Job job = Job.getInstance();
		
		job.setJobName("KmeansSecondarySort");
		job.setJarByClass(KmeansSecondarySortDriver.class);
		job.setMapperClass(KmeansSecondarySortMapper.class);
		job.setReducerClass(KmeansSecondarySortReducer.class);
		
		job.setMapOutputKeyClass(TaxiIDDateKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(TaxiIDKeyPartitioner.class);
		job.setGroupingComparatorClass(TaxiIDKeyGroupingComparatort.class);
		
		job.setOutputKeyClass(TaxiIDDateKey.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}
	
}
