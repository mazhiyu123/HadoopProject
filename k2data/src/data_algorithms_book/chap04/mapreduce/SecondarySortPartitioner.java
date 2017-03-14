package data_algorithms_book.chap04.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object>{
	@Override
	public int getPartition(PairOfStrings key, Object value, int numberOfPartitions) {
		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE)% numberOfPartitions;
	}
}
