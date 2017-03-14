package data_algorithms_book.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	static final String DATE_FORMAT = 	"yyyy-MM-dd";
	static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT);

	public static Date getDate(String dateAsString) {
		try {
			return SIMPLE_DATE_FORMAT.parse(dateAsString);
		} catch (Exception e) {
			return null;
		}
	}
	
	public static long getDateAsMilliSeconds(Date date) {
		return date.getTime();
	}
	
	public static long getDateAsMilliSeconds(String dateAsString) {
		Date date =  getDate(dateAsString);
		return date.getTime();
	}
	
	public static String getDateAsString(long timestamp) {
		return SIMPLE_DATE_FORMAT.format(timestamp);
	}
}
