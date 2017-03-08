package com.mzy.Kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.common.IOUtils;


public class KmeansDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new KmeansDriver(), args);
	}
	
	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException  {
		int i = 0;
		while (true) {
			runJob(args, false);
			System.out.println("第" + (++i) + "次聚类计算");
			if(isFinished("hdfs://ns1/practice/Kmeans/KmeansCenterData/KmeansCenter.txt", args[1], 0.0, args[0])) {
				break;
			}
		}
		return runJob(args, true);
	}
	
	public  int runJob(String[] args, boolean isLastRun) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		
		job.setJobName("KmeansDriver");
		job.setJarByClass(KmeansDriver.class);
		
		job.setMapperClass(KmeansMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//根据最终聚类中心文件， 最后将数据集分类，此时不需要Reduce
		if (!isLastRun) {
			job.setReducerClass(KmeansReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		}
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	// 是否收敛比较函数
	public static boolean isFinished(String oldCenterDataPath, String newCenterDataPath, Double convergence, String dataInputPath) throws IOException {
		ArrayList<ArrayList<Double>>	oldCenterDataList = getCenterDataFromHDFS(oldCenterDataPath, false);
		ArrayList<ArrayList<Double>>	newCenterDataList = getCenterDataFromHDFS(newCenterDataPath, true);
		
		// 处理出现空簇的情况
		if (oldCenterDataList.size() != newCenterDataList.size()) {
			newCenterDataList = fixEmptyClusters(newCenterDataList, newCenterDataPath, dataInputPath, oldCenterDataList.size());
		}
		
		// 聚类中心点个数
		int centerNum = newCenterDataList.size();
		// 计算的维度 这里是两个，经度和纬度
		int dimensionNum = newCenterDataList.get(0).size();
		double distance = 0.0;
		//System.out.println("聚类中心点个数：" + centerNum + " |---| 维度： " + dimensionNum);
		//System.out.println("旧的中心点：" + oldCenterDataList.toString());
		//System.out.println("新的中心点：" + newCenterDataList.toString());
		for(int i = 0; i < centerNum; i++) {
			for(int j = 0; j < dimensionNum; j++) {
				double oldData = Math.abs(oldCenterDataList.get(i).get(j));
				double newData = Math.abs(newCenterDataList.get(i).get(j));
				distance += Math.pow((oldData - newData) / (oldData + newData), 2);
			}
		}
		System.out.println("收敛距离： " + distance);
		//convergence:收敛  
		if(distance <= convergence) {
			// 收敛，得到最终的聚类中心点，删除输出目录，准备最后一次聚类划分
			delOutDir(newCenterDataPath);
			return true;
		} else {
			//没有收敛，在聚类中心文件目录下：删除老的聚类中心文件，将新的聚类中心文件复制到这个目录下，删除输出目录
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			FileStatus[] fileList = hdfs.listStatus(new Path(newCenterDataPath));
			for (int i = 0;i < fileList.length; i++){
				FSDataInputStream in =hdfs.open(fileList[i].getPath());
				FSDataOutputStream out = hdfs.create(new Path(oldCenterDataPath));
				IOUtils.copyBytes(in, out, 4096, true);
			}			
			delOutDir(newCenterDataPath);
		}
		return false;
	}
	
	/* 修复出项空簇的情况
	 * 两种处理方式
	 * 一种是：选择一个距离当前任何质心最远的点，这将消除当前对总平方误差影响最大的点
	 * 另一种处理方式是：从平方差最大的簇中随机选择一个聚类中心替代出现空簇的地方，
	 * 选择一个距离当前任何质心最远的点，这将消除当前对总平方误差影响最大的点
	 */
	public static ArrayList<ArrayList<Double>> fixEmptyClusters(ArrayList<ArrayList<Double>> newCenterDataList,String newCenterDataPath, String dataInputPath, int oldCenterLength) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path newCenterPath = new Path(newCenterDataPath);
		FileStatus[] fileList = hdfs.listStatus(newCenterPath);
		ArrayList<Integer> centerIDList = new ArrayList<Integer>();
		ArrayList<Integer> emptyCenterIDList = new ArrayList<Integer>();
		
		// 得到新聚类中心的ID列表
		for(int i = 0; i < fileList.length; i++) {
			Path centerPath = fileList[i].getPath();
			FSDataInputStream inStream = hdfs.open(centerPath);
			Text lineInfo = new Text();
			LineReader lineReader = new LineReader(inStream, conf);
			while(lineReader.readLine(lineInfo) > 0) {
				String[] coordInfo = lineInfo.toString().split(",");
				centerIDList.add(Integer.parseInt(coordInfo[0]));
			}
		}
		//System.out.println("CenterID: " + centerIDList.toString());
		
		// 找到空簇的ID
		for (int i = 1; i < oldCenterLength; i++) {
			if (centerIDList.get(i-1) != i) {
				emptyCenterIDList.add(i);
			}
		}
		
		if(centerIDList.get(centerIDList.size()-1) != (oldCenterLength)) {
			emptyCenterIDList.add(oldCenterLength);
		}
		
		
		//System.out.println("空簇的ID: " + emptyCenterIDList.toString());
		//这里处理空簇的方式并不严格
		ArrayList<Double> tmpList = new ArrayList<Double>();
		tmpList.add(104.113321);
		tmpList.add(30.670769);
		
		for(int i = 0; i < emptyCenterIDList.size(); i++) {
			newCenterDataList.set(emptyCenterIDList.get(i) - 1, tmpList);
		}
		
		return newCenterDataList;
		// 读取DataPath的数据,计算与每个聚类中心的点，选择出距离任何中心最远的点，返回替换的点
		/*Path dataPath = new Path(dataInputPath);
		FileStatus file*/
	}
	
	// 删除作业输出目录，（新聚类中心的目录）
	public static void delOutDir(String delPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(delPath), true);
		
	}

	/* 从HDFS上读取聚类中心文件解析为List
	 * 可以使用这个函数读取旧的聚类中心数据和新的聚类中心数据，但是读取数据的时候
	 * 旧的聚类文件路径具体到了文件名hdfs://ns1/practice/Kmeans/KmeansCenterData/KmeansCenter.txt
	 * 新的聚类文件路径只是具体到了目录名hdfs://192.168.102.10:9000/practice/Kmeans/KmeansTestData/output/
	 * 因此在读取数据的时候需要判断时候是目录
	 */
	public static ArrayList<ArrayList<Double>> getCenterDataFromHDFS(String centerDataPath, boolean isReadNewCenter) throws IOException {
		ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
		Configuration conf = new Configuration();
		Path path = new Path(centerDataPath);
		FileSystem hdfs = path.getFileSystem(conf);
		System.out.println("从HDFS上读取聚类中心文件解析为List-openPath");
		
		if(isReadNewCenter) {
			FileStatus[] fileList = hdfs.listStatus(path);
			for(int i = 0; i < fileList.length; i++) {
				result.addAll(getCenterDataFromHDFS(fileList[i].getPath().toString(), false));
			}
			return result;
		}
		FSDataInputStream inputStream = hdfs.open(path);
		LineReader lineReader = new LineReader(inputStream, conf);
		Text lineInfo = new Text();
		while(lineReader.readLine(lineInfo) > 0) {
			String[] coordInfo = lineInfo.toString().split(",");
			ArrayList<Double> coorList = new ArrayList<Double>();
			coorList.add(Double.parseDouble(coordInfo[1]));
			coorList.add(Double.parseDouble(coordInfo[2]));
			result.add(coorList);
		}
		lineReader.close();
		return result;
	}

}
