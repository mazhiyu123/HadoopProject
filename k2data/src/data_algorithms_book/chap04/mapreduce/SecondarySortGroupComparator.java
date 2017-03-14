package data_algorithms_book.chap04.mapreduce;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class SecondarySortGroupComparator implements RawComparator<PairOfStrings>{
	// 仅通过userID分组
	@Override
	public int compare(PairOfStrings first, PairOfStrings second) {
		return first.getLeftElement().compareTo(second.getLeftElement());
	}
	
	// 由于RawComparator接口可以在字节的层面上比较对象的大小，一次需要多实现一个在字节数组层面
	// 比较大小的方法compare()
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInputBuffer buffer = new DataInputBuffer();
		PairOfStrings a = new PairOfStrings();
		PairOfStrings b = new PairOfStrings();
		
		try {
			buffer.reset(b1,s1,l1);
			a.readFields(buffer);
			buffer.reset(b2, s2, l2);
			b.readFields(buffer);
			return compare(a,b);
		} catch (Exception e) {
			return -1;
		}
	}
}
