package com.mzy.k2data.v2;

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
	
	
	
	public static class DataFileMap extends  Mapper<LongWritable, Text, Text, Text>
	{
		//为每一行数据添加的行标识符
		int row=0;
		String[] range_k;
		String[] range_value;
	     /*
         * 用来获取分布式缓存
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        	super.setup(context);
            if (context.getCacheFiles() != null&& context.getCacheFiles().length > 0) {
            	//取得缓存的文件
                File cacheFile = new File("./cachePath");
                InputStreamReader read = new InputStreamReader(new FileInputStream(cacheFile));
                BufferedReader bufferedReader = new BufferedReader(read);
                
                //读一行数据，切分
                String line;
                int ri=0;
				while ((line = bufferedReader.readLine()) != null) {
					 range_k[ri]=line.substring(0, 1);
					 range_value[ri]=line.substring(2);
					 ri++;
				}
				bufferedReader.close();
            }else {
            	System.out.println("读取分布式缓存失败");
            }
        }
		
		
		
		
		public void map(LongWritable line, Text value,Context context)throws IOException, InterruptedException
		{
			String outKey;
			String outValue;
			//记录正常数据
			StringBuilder resnormal=new StringBuilder("deviceId");
			//记录异常数据
			StringBuilder resabnormal=new StringBuilder("deviceId");
			row++;
			//是否整行输出
			boolean tag=true;
			
			//去掉每一行开头的字母
			String newvalue=value.toString().substring(3);
			
			for(int i=0;i<range_k.length;i++){
				if(row == Integer.valueOf(range_k[i]))tag=false;
			}
			
			if(tag){
				//整行输出的情况
				context.write(new Text("abnormal"), new Text(newvalue));
			}else{
				//不一定是整行输出，需要判断和过滤
				boolean restag=false;
				String[] datatokens=SeparateDataMain.DELIMITER.split(newvalue);
				int min=Integer.valueOf(range_value[0]);
				int max=Integer.valueOf(range_value[1]);
				for(int i=0;i<datatokens.length;i++){
					int tmpdata=Integer.valueOf(datatokens[i]);
					if(tmpdata>min&&tmpdata<max){
						restag=true;
						resnormal.append(datatokens[i]+",");
					}else{
						resabnormal.append(datatokens[i]+",");
					}
				}
				if(restag){
					//正常数据输出
					context.write(new Text("normal"), new Text(resnormal.toString()));
				}else{
					context.write(new Text("abnormal"), new Text(resabnormal.toString()));
				}
			}
			
		}

	}

	
	/*
	 * 进行正常数据和异常数据的区分
	 */
	public static class ResReduce extends  Reducer<Text, Text, Text, NullWritable>
	{	
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			//自定义计数器 正常数据计数
			Counter normalCounter=(Counter) context.getCounter(ResultEnum.NORMALCOUNTER);
			//异常数据技术
			Counter abnormalCounter=(Counter) context.getCounter(ResultEnum.ABNORMALCOUNTER);
			String outKey;
			
			for(Text val:values){
				String tmp=val.toString();
				outKey=tmp.substring(0, tmp.length()-1);
				context.write(new Text(outKey), null);
				if("normal".equals(key.toString())){
					normalCounter.increment(1);
				}else{
					abnormalCounter.increment(1);
				}
			}
			
			
			}
			
		}
		
		
	}
	
		
	

