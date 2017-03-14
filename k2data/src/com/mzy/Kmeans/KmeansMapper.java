package com.mzy.Kmeans;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	public List<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();
	
	protected void setup(Context context) throws IOException {
		// 文件系统的选择
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(context.getConfiguration());

		Path inPath = new Path("hdfs://192.168.102.10:9000/practice/Kmeans/KmeansCenterData/KmeansCenter.txt");
		//Path inPath = new Path("/newdisk/Kmeans-data/KmeansTestData.txt");
		FSDataInputStream fsInput = hdfs.open(inPath);
		LineReader lineInput = new LineReader(fsInput, conf);
		Text line = new Text();
		ArrayList<Double> centerList = new ArrayList<Double>();
		
		while(lineInput.readLine(line) > 0) {
			String[] centerInfo = line.toString().split(",");
			centerList.add(Double.parseDouble(centerInfo[0]));
			centerList.add(Double.parseDouble(centerInfo[1]));
		}
		centers.add(centerList);
	}
	
	public Double computeDist(Double currentLongitude, Double currentLatitude, ArrayList<Double> centers) { 
		Double centerLongitude = centers.get(0);
		Double centerLatitude = centers.get(1);
		
		Double diffLongitude = Math.pow((currentLongitude - centerLongitude), 2);
		Double diffLatitude = Math.pow((currentLatitude - centerLatitude), 2);
		Double euclDist = Math.sqrt(diffLongitude + diffLatitude);
		return euclDist;
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//
		String[]  coordinateInfo = value.toString().split(",");
		Double currentLongitude = Double.parseDouble(coordinateInfo[1]);
		Double currentLatitude = Double.parseDouble(coordinateInfo[2]);
		Double minDist = Double.MAX_VALUE;
		int centerIndex = centers.size();
		
		for (int i = 0; i < centerIndex; i++) {
			Double currentDist = computeDist(currentLongitude, currentLatitude, centers.get(i));
			if (currentDist < minDist) {
				minDist = currentDist;
				centerIndex = i;
			}
		}
		context.write(new IntWritable(centerIndex), new Text(currentLongitude + "," + currentLatitude));
	}
}
