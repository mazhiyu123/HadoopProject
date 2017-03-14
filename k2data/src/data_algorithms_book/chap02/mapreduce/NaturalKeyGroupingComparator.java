package data_algorithms_book.chap02.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator{
	protected NaturalKeyGroupingComparator() {}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		CompositeKey ck1 = (CompositeKey) wc1;
		CompositeKey ck2 = (CompositeKey) wc2;
		return ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
	}
}
