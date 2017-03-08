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

		Path inPath = new Path("hdfs://ns1/practice/Kmeans/KmeansCenterData/KmeansCenter.txt");
		//Path inPath = new Path("/newdisk/Kmeans-data/KmeansTestData.txt");
		FSDataInputStream fsInput = hdfs.open(inPath);
		LineReader lineInput = new LineReader(fsInput, conf);
		Text line = new Text();
		
		int j = 0;
		while(lineInput.readLine(line) > 0) {
			//System.out.println("J: " + j++);
			//System.out.println("LineInput: " + line.toString());
			ArrayList<Double> centerList = new ArrayList<Double>();
			String[] centerInfo = line.toString().split(",");
			centerList.add(Double.parseDouble(centerInfo[1]));
			centerList.add(Double.parseDouble(centerInfo[2]));
			//System.out.println("centerList: " + centerList.toString());
			centers.add(centerList);
			//line.clear();
			//line.set(new byte[0]);
			//line = null;
		}
		lineInput.close();
	}
	
	public Double computeDist(double currentLongitude, double currentLatitude, ArrayList<Double> centers) { 
		double centerLongitude = centers.get(0);
		double centerLatitude = centers.get(1);
		//System.out.println("currentLongitude: " + currentLongitude + " currentLatitude: " + currentLatitude);
		//System.out.println("centerLongitude: " + centerLongitude + " centerLatitude: " + centerLatitude);
		double diffLongitude = Math.pow((currentLongitude - centerLongitude), 2);
		double diffLatitude = Math.pow((currentLatitude - centerLatitude), 2);
		double euclDist = Math.sqrt(diffLongitude + diffLatitude);
		return euclDist;
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//
		String[]  coordinateInfo = value.toString().split(",");
		double currentLongitude = Double.parseDouble(coordinateInfo[1]);
		double currentLatitude = Double.parseDouble(coordinateInfo[2]);
		double minDist = Double.MAX_VALUE;
		int centerIndex = centers.size();
		
		//System.out.println("centerIndexLength: " + centerIndex);
		/*for (int i = 0; i < centers.size(); i++) {
			System.out.println("centerIndex: " + centers.get(i).toString());
		}*/
		
		
		for (int i = 0; i < centers.size(); i++) {
			Double currentDist = computeDist(currentLongitude, currentLatitude, centers.get(i));
			//System.out.println("minDist: " + minDist + " :-----currentDist: " + currentDist);
			if (currentDist < minDist) {
				minDist = currentDist;
				centerIndex = (i+1);
			}
		}
		
		//System.out.println("\n" + centerIndex + " : " + currentLongitude + "," + currentLatitude + "\n");
		context.write(new IntWritable(centerIndex), new Text(currentLongitude + "," + currentLatitude));
	}
}
