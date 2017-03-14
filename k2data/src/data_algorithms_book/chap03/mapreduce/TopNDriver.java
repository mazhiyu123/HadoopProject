package data_algorithms_book.chap03.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import data_algorithms_book.util.HadoopUtil;

public class TopNDriver extends Configured implements Tool{
	private static Logger THE_LOGGER = Logger.getLogger(TopNDriver.class);
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
	         THE_LOGGER.warn("usage TopNDriver <N> <input> <output>");
	         System.exit(1);
	      }
		  THE_LOGGER.info("N="+args[0]);
	      THE_LOGGER.info("inputDir="+args[1]);
	      THE_LOGGER.info("outputDir="+args[2]);
		
	      int returnStatus = ToolRunner.run(new TopNDriver(), args);
	      System.exit(returnStatus);
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf());
		HadoopUtil.addJarsToDistributedCache(job, "/lib/");
		int N = Integer.parseInt(args[0]);
		
		//SequenceFileInputFormat主要处理SequenceFile和MapFile
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(TopNMapper.class);
	      job.setReducerClass(TopNReducer.class);
	      job.setNumReduceTasks(1);

	      // map()'s output (K,V)
	      job.setMapOutputKeyClass(NullWritable.class);   
	      job.setMapOutputValueClass(Text.class);   
	      
	      // reduce()'s output (K,V)
	      job.setOutputKeyClass(IntWritable.class);
	      job.setOutputValueClass(Text.class);

	      FileInputFormat.addInputPath(job, new Path(args[1]));
	      FileOutputFormat.setOutputPath(job, new Path(args[2]));
	      
	      boolean status = job.waitForCompletion(true);
	      THE_LOGGER.info("run(): status="+status);
	      return status ? 0 : 1;
	}
}
