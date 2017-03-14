package data_algorithms_book.chap02.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import data_algorithms_book.util.DateUtil;

public class SecondarySortReducer extends Reducer<CompositeKey, NaturalValue, Text, Text>{
	
	public void reduce(CompositeKey key,
						Iterable<NaturalValue> values,
						Context context) throws IOException, InterruptedException {
	StringBuilder builder = new StringBuilder();
	for (NaturalValue data : values) {
		builder.append("(");
		String dateAsString = DateUtil.getDateAsString(data.getTimestamp());
		double price = data.getPrice();
		builder.append(dateAsString);
		builder.append(",");
		builder.append(price);
		builder.append(")");
	}
	context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
	}
}
