package com.mzy.k2data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.lang.reflect.Field;  

public class MR {
	/*
	 * 处理来自data-input.csv的数据
	 * 例子：
	 * Map输入数据：did1,1.1,1.2,1.3
	 * Map输出数据：key:1  value:datafile:1,1.1,1.2,1.3
	 */
	public static class DataFileMap extends  Mapper<LongWritable, Text, Text, MyDataType>
	{
		//为每一行数据添加的行标识符
		int row=0;
		public void map(LongWritable line, Text value,Context context)throws IOException, InterruptedException
		{
			//去掉每一行开头的字母
			String newvalue=value.toString().substring(3);
			//输出的value
			MyDataType data_v=new MyDataType("datafile",newvalue);
			//输出的key
			Text data_k=new Text((String.valueOf(++row)));
			context.write(data_k, data_v);
		}

	}

	/*
	 * 处理来自range-input.csv的数据
	 * 例子：
	 * Map输入数据：2，2，4
	 * Map输出数据：key:2  value:2,4
	 */
	public static class RangeFileMap extends  Mapper<LongWritable, Text, Text, MyDataType>
	{
		public   String  range_k=null;
        public   String  newvalue=null;
        
        /*
         * 用来获取分布式缓存
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, MyDataType>.Context context)
                throws IOException, InterruptedException {
        	super.setup(context);
            if (context.getCacheFiles() != null&& context.getCacheFiles().length > 0) {
            	//取得缓存的文件
                File cacheFile = new File("./cachePath");
                InputStreamReader read = new InputStreamReader(new FileInputStream(cacheFile));
                BufferedReader bufferedReader = new BufferedReader(read);
                
                //读一行数据，切分
                String line;
				while ((line = bufferedReader.readLine()) != null) {
					 range_k=line.substring(0, 1);
	                 newvalue=line.substring(2);
				}
				bufferedReader.close();
            }else {
            	System.out.println("读取分布式缓存失败");
            }
        }
        

		public void map(LongWritable line, Text value,Context context)throws IOException, InterruptedException
		{	 
			 //输出的key
			 range_k=value.toString().substring(0, 1);
			 
             newvalue=value.toString().substring(2);
             //输出的value
             MyDataType range_v=new MyDataType("rangefile",newvalue);
			
			 context.write(new Text(range_k), range_v);
		}

	}
	

	/*
	 * 进行正常数据和异常数据的区分
	 */
	public static class ResReduce extends  Reducer<Text, MyDataType, Text, NullWritable>
	{	
		//设置多路输出
		 private MultipleOutputs<Text, NullWritable> outPath;
		 public void setup(Context context) {
		   outPath = new MultipleOutputs<Text, NullWritable>(context);
		 }
		
		
		public void reduce(Text fileName, Iterable<MyDataType> values, Context context) throws IOException, InterruptedException
		{
			
			// 分别记录来自两个文件的数据
			ArrayList<Float> fromDataFile=new ArrayList<Float>();
			ArrayList<Float> fromRangeFile=new ArrayList<Float>();
			//记录输入数据的长度
			int dataLength=-1;
			int rangeLength=-1;
		
			for(MyDataType data:values){
				if ("datafile".equals(data.getFlag()))
				{//处理来自DataFileMap的数据
					String[] datatokens=SeparateDataMain.DELIMITER.split(data.getValue());
					dataLength=datatokens.length;
					for(int i=0;i<dataLength;i++){
						fromDataFile.add(Float.parseFloat(datatokens[i]));
					}
				} else if ("rangefile".equals(data.getFlag()))
				{//处理来自RangeFileMap的数据
					String[] rangetokens=SeparateDataMain.DELIMITER.split(data.getValue());
					rangeLength=rangetokens.length;
					for(int i=0;i<rangeLength;i++){
						fromRangeFile.add(Float.parseFloat(rangetokens[i]));
					}
				}
			}
			
			
			//自定义计数器 正常数据计数
			Counter normalCounter=(Counter) context.getCounter(ResultEnum.NORMALCOUNTER);
			//异常数据技术
			Counter abnormalCounter=(Counter) context.getCounter(ResultEnum.ABNORMALCOUNTER);
			
			if(rangeLength != -1){
				 //处理range-input.csv文件中有的需要筛选的一行数据
				 
				
				//确定正常数据的范围
				float min=fromRangeFile.get(0);
				float max=fromRangeFile.get(1);
				
				//记录正常数据
				StringBuilder res1=new StringBuilder("deviceId");
				//记录异常数据
				StringBuilder res2=new StringBuilder("deviceId");
				
				
				//表明某一行是否是一部分是正常数据，另一部分是异常数据的情况
				boolean tag=false;
				
				for(int i=0;i<dataLength;i++){
					float tmpdata=fromDataFile.get(i);
					if(tmpdata>=min&&tmpdata<=max){
						//连接正常数据
						res1.append(String.valueOf(tmpdata)+",");
					}else{
						//连接异常数据
						tag=true;
						res2.append(String.valueOf(tmpdata)+",");
					}
				}
				
				//输出正常数
				outPath.write("normal", new Text(res1.toString().substring(0,res1.length()-1)), null,"/k2data/res-output/normal/");
				normalCounter.increment(1);
				if(tag){
					//输出异常数
					outPath.write("abnormal", new Text(res2.toString().substring(0,res2.length()-1)), null,"/k2data/res-output/abnormal/");
					abnormalCounter.increment(1);
				}
			}else{
                //处理range-input.csv没有指定行的一行数据，即整行数据都是异常数据
				StringBuilder res3=new StringBuilder("deviceId");
				for(int i=0;i<dataLength;i++){
					float tmpdata=fromDataFile.get(i);
						res3.append(String.valueOf(tmpdata)+",");
					}	
				//异常数据路径
				outPath.write("abnormal", new Text(res3.toString().substring(0,res3.length()-1)), null,"/k2data/res-output/abnormal/");
				abnormalCounter.increment(1);
				
			}
			
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			   outPath.close();
			 }
		
	}
	
		
	
}
