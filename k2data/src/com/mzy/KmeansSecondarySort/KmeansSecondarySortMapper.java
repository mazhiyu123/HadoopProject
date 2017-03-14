package com.mzy.KmeansSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansSecondarySortMapper extends Mapper<LongWritable, Text, TaxiIDDateKey, Text>{
	
	private final TaxiIDDateKey mapOutKey = new TaxiIDDateKey();
	//OtherInfoValue mapOutValue = new OtherInfoValue();
	//String mapOutValue;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String recordInfo = value.toString();
		String[] lines = recordInfo.split(",");
		/*System.out.println("\n**************\n");
		for(int i = 0; i < recordInfo.length; i++) {
			System.out.println(recordInfo[i]);
		}*/
		
		/*TaxiIDDateKey mapOutKey = new TaxiIDDateKey(recordInfo[0], recordInfo[4]);
		OtherInfoValue mapOutValue = new OtherInfoValue(recordInfo[3], recordInfo[2], recordInfo[1]);
		*/
		mapOutKey.setTaxiID(Integer.parseInt(lines[0]));
		mapOutKey.setDate(lines[4]);
		
		StringBuilder builder = new StringBuilder();
		builder.append(lines[3]);
		builder.append(",");
		builder.append(lines[2]);
		builder.append(",");
		builder.append(lines[1]);
		//mapOutValue = builder.toString()
		/*mapOutValue.setDimension("setDimension");
		mapOutValue.setLongitude("setLongitude");
		mapOutValue.setTaxiState("setTaxiState");
		mapOutKey.setDate("setDate")*/;
		
		// System.out.println(mapOutKey.toString() + "\n" + mapOutValue.toString());
		context.write(mapOutKey, new Text(builder.toString()));
	}
}
