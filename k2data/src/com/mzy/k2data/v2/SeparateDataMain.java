package com.mzy.k2data.v2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class SeparateDataMain extends Configured implements Tool {
	//分隔符
	public static final Pattern DELIMITER = Pattern.compile("[,]");
	
	//正常和异常数据输出路径
	public   String normalPath;
	public   String abnormalPath;
	public  int testint;
		public static void main(String args[]) throws Exception
		{  
			/*if(args.length !=5 ){
				System.out.println("需要传递五个参数");
				System.exit(0);
			}*/
			
		 ToolRunner.run(new Configuration(), new SeparateDataMain(), args);
		}
		
		
		public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
			
		  //range文件的本地目录
		  String localPath="/home/master/k2data/range/range-input.csv";
		  //data文件hdfs路径
		  String dataFilePath="/k2data/data/";
		  
		 // normalPath=args[3];
	      //abnormalPath=args[4];
	     // testint=100;
		  
		  String outPath="/k2data/res-output";
		  // range文件上传到hdfs的路径
	      String rangeFilePath="hdfs://ns1/k2data/range/";
		    
		  Configuration conf= getConf();
		  Job job = Job.getInstance(conf, "K2DATA");
		  //将range-input.csv从本地目录上传的HDFS中
		  putLocalFile(localPath,rangeFilePath,conf);
		  
		  
		  job.setJarByClass(SeparateDataMain.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setReducerClass(MR.ResReduce.class);
		  job.setPartitionerClass(MyPartitioner.class);
		  job.setNumReduceTasks(2);
		  //cache的路径
		  job.addCacheFile(new  URI("/k2data/range/range-input.csv#cachePath"));
		  //多路输入设置
		  MultipleInputs.addInputPath(job, new Path(dataFilePath), TextInputFormat.class, MR.DataFileMap.class);
		 // MultipleInputs.addInputPath(job, new Path(rangeFilePath), TextInputFormat.class, MR.RangeFileMap.class);
		  FileOutputFormat.setOutputPath(job, new Path(outPath));
		  //多路输出设置
		  //MultipleOutputs.addNamedOutput(job,args[3], TextOutputFormat.class, Text.class,NullWritable.class );
		  //MultipleOutputs.addNamedOutput(job,args[4], TextOutputFormat.class, Text.class,NullWritable.class );
		  return job.waitForCompletion(true) ? 0 : 1;
		 
		}
		
		//方法实现功能：将range-input.cvs文件从本地文件系统上传
		public void putLocalFile(String local, String des,Configuration conf) throws IOException {
		        FileSystem hdfs = null;
		        try {
		            hdfs = FileSystem.get(conf);
		            Path srcPath = new Path(local);
		            Path dstPath = new Path(des);
		            hdfs.copyFromLocalFile(srcPath, dstPath);
		        } catch (IOException e) {
		            e.printStackTrace();
		        } finally{
		            if(hdfs != null){
		                try {
		                    hdfs.close();
		                } catch (IOException e) {
		                    e.printStackTrace();
		                }
		            }
		        }
	    }
}	
		
		
