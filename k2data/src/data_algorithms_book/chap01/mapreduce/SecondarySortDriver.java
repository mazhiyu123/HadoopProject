package data_algorithms_book.chap01.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class SecondarySortDriver extends Configuration implements Tool {
	private static Logger theLogger = Logger.getLogger(SecondarySortDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job =Job.getInstance(conf);
		job.setJarByClass(SecondarySortDriver.class);
		job.setJobName("SecondarySortDriver");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(DateTemperaturePair.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setPartitionerClass(DateTemperaturePartitioner.class);
		job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);
		
		boolean status = job.waitForCompletion(true);
		theLogger.info("run() status: " + status);
		return status ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			theLogger.warn("SecondarySortDriver <input-dir> <output-dir>");
			throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
		}
		
		int returenStatus = submitJob(args);
		theLogger.info("returenStatus: " + returenStatus);
		
		System.exit(returenStatus);
	}
	
	public static int submitJob(String[] args) throws Exception {
		int retureStatus = ToolRunner.run(new SecondarySortDriver(), args);
		return retureStatus;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}
}
