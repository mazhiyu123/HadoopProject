package com.mzy.hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text,Text,Text,Text> {
	public static final String LEFT_FILENAME = "student_info.txt";
	public static final String RIGHT_FILENAME = "student_class_info.txt";
	public static final String LEFT_FILENAME_FLAG = "l";
	public static final String RIGHT_FILENAME_FLAG = "r";
	
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		Iterator<Text> itr = value.iterator();
		List<String> studentClassName = new ArrayList<String>();
		String studentName = "";
		
		while(itr.hasNext()){
			String[] infos = itr.next().toString().split("\t");
			if (infos[1].equals(LEFT_FILENAME_FLAG)){
				studentName = infos[0];
			} else if (infos[1].equals(RIGHT_FILENAME_FLAG)){
				studentClassName.add(infos[0]);
			}
		}
		
		for (int i = 0; i < studentClassName.size(); i++){
			context.write(new Text(studentName), new Text(studentClassName.get(i)));
		}
	}
}
