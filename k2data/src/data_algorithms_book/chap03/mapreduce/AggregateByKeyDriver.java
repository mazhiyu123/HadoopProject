package data_algorithms_book.chap03.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import data_algorithms_book.util.HadoopUtil;

public class AggregateByKeyDriver extends Configured implements Tool{
	   private static Logger THE_LOGGER = Logger.getLogger(AggregateByKeyDriver.class);
	   
	   public int run(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		   Job job = Job.getInstance(getConf());
		   
		   HadoopUtil.addJarsToDistributedCache(job, "/lib/");
		   job.setJobName("AggregateByKeyDriver");
		   
		   job.setInputFormatClass(TextInputFormat.class);
		      job.setOutputFormatClass(SequenceFileOutputFormat.class);

		      job.setOutputKeyClass(Text.class);
		      job.setOutputValueClass(IntWritable.class);

		      job.setMapperClass(AggregateByKeyMapper.class);
		      job.setReducerClass(AggregateByKeyReducer.class);
		      job.setCombinerClass(AggregateByKeyReducer.class);
		      
		      FileInputFormat.addInputPath(job, new Path(args[0]));
		      FileOutputFormat.setOutputPath(job, new Path(args[1]));
		      
		      boolean status = job.waitForCompletion(true);
		      THE_LOGGER.info("run(): status="+status);
		      return status ? 0 : 1;
	   }
	   
	   public static void main(String[] args) throws Exception {
		      // Make sure there are exactly 2 parameters
		      if (args.length != 2) {
		         THE_LOGGER.warn("usage AggregateByKeyDriver <input> <output>");
		         System.exit(1);
		      }

		      THE_LOGGER.info("inputDir="+args[0]);
		      THE_LOGGER.info("outputDir="+args[1]);
		      int returnStatus = ToolRunner.run(new AggregateByKeyDriver(), args);
		      System.exit(returnStatus);
		   }
}
