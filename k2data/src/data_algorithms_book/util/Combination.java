package data_algorithms_book.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Combination {
	public static < T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
		List<List<T>> result = new ArrayList<List<T>>();
		for (int i = 0; i <= elements.size(); i++) {
			result.addAll(findSortedCombinations(elements, i));
		}
		return result;
	}
	
	// 递归
	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
		List<List<T>> result = new ArrayList<List<T>>();
		if ( n == 0) {
			result.add(new ArrayList<T>());
			return result;
		}
		
		List<List<T>> combinations = findSortedCombinations(elements, n-1);
		for (List<T> combination : combinations) {
			for (T element : elements) {
				if (combination.contains(element)){
					continue;
				}
				List<T> list = new ArrayList<T>();
				list.addAll(combination);
				
				if (list.contains(combination)) {
					continue;
				}
				list.add(element);
				Collections.sort(list);
				
				if (result.contains(list)){
					continue;
				}
				result.add(list);
			}
		}
		return result;
	}
}
