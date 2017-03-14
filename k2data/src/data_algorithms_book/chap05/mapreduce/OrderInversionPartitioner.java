package data_algorithms_book.chap05.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner 
					extends Partitioner<PairOfWords, IntWritable>{

		@Override
		public int getPartition(PairOfWords key, IntWritable value, int numberOfPartitions){
			String keyWord = key.getLeftElement();
			return Math.abs(((int)hash(keyWord))%numberOfPartitions);
		}
		
		private static long hash(String str) {
			long h = 1125899906842597L;
			int length = str.length();
			for( int i = 0; i < length; i++) {
				h = 31*h + str.charAt(i);
			}
			return h;
		}
}
