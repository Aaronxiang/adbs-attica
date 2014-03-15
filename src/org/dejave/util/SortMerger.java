package org.dejave.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public abstract class SortMerger<T> {
	Comparator<T> comparator = null;

	public SortMerger(Comparator<T> comparator) {
		this.comparator = comparator;
	}

	public abstract void mergeValues(T first, T second);
	public abstract ListIterator<T> createFirstIteratorAt(int positionIdx);

	public void doMerge(ListIterator<T> firstIt, ListIterator<T> secondIt) {
		boolean firstGroupStarted = false;
		boolean firstInputEnded = false;
		int firstGroupStartIdx = 0;

		T groupValue = null;
		T firstValue = firstIt.next();
		T secondValue = secondIt.next();

		do {
			int ret = comparator.compare(firstValue, secondValue);
			if (0 != ret) {
				if (firstGroupStarted) {
					if (! firstInputEnded) {
						if (secondIt.hasNext())
							secondValue = secondIt.next();
						else {
							break;
						}
					}

					int ret2 = comparator.compare(groupValue, secondValue);
					if (0 == ret2) {
						firstIt = createFirstIteratorAt(firstGroupStartIdx);
						firstValue = firstIt.next();
						assert(firstValue == groupValue);
						//we could make a join here with this element
					}
					else {
						firstGroupStarted = false;
					}
				}
				else {
					if (ret < 0) {
						if (firstIt.hasNext()) {
							firstValue = firstIt.next();
						}
						else 
							break;
					}
					else if (ret > 0) {
						if (secondIt.hasNext())
							secondValue = secondIt.next();
						else
							break;
					}
				}
			}
			else {
				mergeValues(firstValue, secondValue);

				if (!firstGroupStarted) {
					firstGroupStarted = true;
					firstGroupStartIdx = firstIt.nextIndex() - 1;
					groupValue = firstValue;
				}

				if (firstIt.hasNext()) {
					firstValue = firstIt.next();
				}
				else if (secondIt.hasNext()) {
					secondValue = secondIt.next();
					firstIt = createFirstIteratorAt(firstGroupStartIdx);
					firstValue = firstIt.next();
					firstInputEnded = true;
				}
				else {
					break;
				}
			}
		} while (true);
	}
	
	/**
	 * ########################################
	 * ##### Debug
	 * ########################################
	 */
	
	//class which allows strings to be distinguished with idx
	static class DebugTuple {
		String s;
		Integer idx;
		
		public DebugTuple(String s, Integer idx) {
			this.s = s;
			this.idx = idx;
		}
		
		@Override
		public String toString() {
			return s + " (" + idx + ")";
		}
	}
	
	//comparator compares Tuple strings only
	static public class DebugTupleComparator implements Comparator<DebugTuple> {
		@Override
		public int compare(DebugTuple v0, DebugTuple v1) {
			return v0.s.compareTo(v1.s);
		}
	}
	
	//
	public static class DebugTupleSortMerger extends SortMerger<DebugTuple> {
		ArrayList<String> result = new ArrayList<String>(); 
		List<DebugTuple> first = null;
		List<DebugTuple> second = null;

		public DebugTupleSortMerger(List<DebugTuple> first, List<DebugTuple> second) {
			super(new DebugTupleComparator());
			this.first = first;
			this.second = second;
		}
		
		@Override
		public void mergeValues(DebugTuple first, DebugTuple second) {
			result.add(first.s + "_" + first.idx + " : " + second.s + "_" + second.idx);
		}
		
		public List<String> result() {
			return this.result;
		}

		@Override
		public ListIterator<DebugTuple> createFirstIteratorAt(int positionIdx) {
			ListIterator<DebugTuple> it = null;
			if (0 == positionIdx) {
				it = first.listIterator();
			}
			else {
				it = first.listIterator(positionIdx);
			}
			return it;
		}
		
	}
	
    /**
     * Debug main.
     */
    public static void main (String [] args) {
		//normal case
		String a[] = {"A", "A", "B", "B", "J", "J", "K"};
		String b[] = {"B", "C", "G", "H", "I", "I", "J", "J", "J", "M"};
		checkMergeOfArrays(a, b, 8);
		checkMergeOfArrays(b, a, 8);
		
		//end of group == end of one input
		String c[] = {"A", "B", "B"};
		String d[] = {"A", "B"};
		checkMergeOfArrays(c, d, 3);
		checkMergeOfArrays(d, c, 3);
		
		//beginning/end of group == beginning/end of input; one input takes all the array
		String e[] = {"J", "J", "K"};
		String f[] = {"J", "J", "J"};
		checkMergeOfArrays(e, f, 6);
		checkMergeOfArrays(f, e, 6);
		
		//beginning of group == beginning of input
		String g[] = {"A", "A", "B"};
		String h[] = {"A", "B"};
		checkMergeOfArrays(g, h, 3);
		checkMergeOfArrays(h, g, 3);
	}//main()

	private static void checkMergeOfArrays(String a[], String b[], int joinsNo) {
		List<DebugTuple> first = createTupleList(a);
		List<DebugTuple> second = createTupleList(b);	

		System.out.println("Merging: " + first);
		System.out.println("with   : " + second);
		
		DebugTupleSortMerger merger = new DebugTupleSortMerger(first, second);
		merger.doMerge(first.listIterator(), second.listIterator());
		List<String> result = merger.result();
		assert(result.size() == joinsNo);
		System.out.println("DONE: " + result + ": " + joinsNo + " ?= " + result.size());
		System.out.println("");
	}
	
	private static List<DebugTuple> createTupleList(String[] a) {
		List<DebugTuple> res = new ArrayList<DebugTuple>();
		int interIdx = 0;
		for (int i = 0; i < a.length; ++i) {
			if (0 == i || a[i] != a[i-1]) {
				interIdx = 0;
			}
			else {
				interIdx++;
			}
			res.add(new DebugTuple(a[i], interIdx));
		}
		
		return res;
	}
}