package data_algorithms_book.chap04.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinDriver {
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException, Exception {
		Path transactions = new Path(args[0]);
		Path users = new Path(args[1]);
		Path output = new Path(args[2]);
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(LeftJoinDriver.class);
	    job.setJobName("Phase-1: Left Outer Join");
	    
	    job.setPartitionerClass(SecondarySortPartitioner.class);
	    // natural key 的分组方式
	    job.setGroupingComparatorClass(PairOfStrings.Comparator.class);
	    // PairOfStrings的排序方式
	    job.setSortComparatorClass(PairOfStrings.Comparator.class);
	    
	    job.setReducerClass(LeftJoinReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // 多路输入
	    MultipleInputs.addInputPath(job, transactions, TextInputFormat.class,LeftJoinTransactionMapper.class);
	    MultipleInputs.addInputPath(job, users, TextInputFormat.class,LeftJoinTransactionMapper.class);
	    
	    job.setMapOutputKeyClass(PairOfStrings.class);
	    job.setMapOutputValueClass(PairOfStrings.class);
	    
	    FileOutputFormat.setOutputPath(job, output);
	    
	    if (job.waitForCompletion(true)) {
	    	return ;
	    } else {
	    	throw new Exception("Phase-1: Left Outer Join Job Failed");
	    }
	}
}
