package org.dejave.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public abstract class SortMerger<T> {
	Comparator<T> comparator = null;
	T firstValue = null;
	T secondValue = null;
	
	/**
	 * Constructs new merger with a given comparator.
	 * @param comparator
	 */
	public SortMerger(Comparator<T> comparator) {
		this.comparator = comparator;
	}

	/**
	 * Invoked when there are two values from different inputs to be merged.
	 * @param first - value of the first input;
	 * @param second - value of the second intput.
	 */
	public abstract void mergeValues(T first, T second);
	
	/**
	 * Invoked to create new iterator at positionIdx.
	 * @param positionIdx index of the first element to be returned from the list iterator (by a call to next)
	 * @return
	 */
	public abstract ListIterator<T> createFirstIteratorAt(int positionIdx);

	public void doMerge(ListIterator<T> firstIt, ListIterator<T> secondIt) {
		//first and last index of (inclusive) of equal-values group in the first input
		int firstGroupStartIdx = 0;
		int firstGroupLastIdx = 0;
		//value of the equal-values group
		T groupValue = null;
		
		//if some of the inputs has no values, we are done
		if (!firstIt.hasNext() || !secondIt.hasNext())
			return;
			
		//otherwise set them to first positions
		firstValue = firstIt.next();
		secondValue = secondIt.next();

		int ret = -1;//something different than 0, to force first comparison
		do {
			if (0 == ret) {
				//values are same - merge them
				mergeValues(firstValue, secondValue);

				//remember equal-values group parameters
				firstGroupStartIdx = firstIt.previousIndex();
				groupValue = firstValue;

				int cmpRes = 0;
				//iterate over the first tuples until the different one is found
				while (firstIt.hasNext()) {
					firstValue = firstIt.next();
					cmpRes = comparator.compare(firstValue, groupValue);
					if (0 == cmpRes) {
						//we are still in an equal-values group -> second tuple is also equal to current first input, thus merge
						mergeValues(firstValue, secondValue);
					}
					else 
						break;
				}
				
				//reached the end of a group, two cases may occur
				//	- end of the group coincides with end of the input stream (!hasNext() and 0 == cmpRes); then group is [firstGroupStartIdx, firstIt.nextIndex()] (inclusive)
				//	- otherwise, then group is [firstGroupStartIdx, firstIt.previousIndex()] (inclusive)
				boolean firstIsDone = !firstIt.hasNext() && 0 == cmpRes;
				firstGroupLastIdx = firstIsDone ? firstIt.nextIndex() : firstIt.previousIndex();
				
				while (secondIt.hasNext()) {
					secondValue = secondIt.next();
					//if the following second input values are equal to the equal-values group value, merge them without value checking against first (since they are equal)
					cmpRes = comparator.compare(groupValue, secondValue);
					if (0 == cmpRes) {
						if (firstGroupStartIdx == firstGroupLastIdx ) {//no need to create iterator
							mergeValues(groupValue, secondValue);
						}
						else {//more items in a group to iterate, thus iterator is needed
							ListIterator<T> fIt = createFirstIteratorAt(firstGroupStartIdx);
							while (fIt.nextIndex() < firstGroupLastIdx) {
								mergeValues(fIt.next(), secondValue);
							}
						}
					}
					else 
						break;
				}
				
				//if the first input was done and there is nothing in the second one, we are done
				if (firstIsDone && !secondIt.hasNext()) 
					break;
			}

			ret = comparator.compare(firstValue, secondValue);
		} while (advancePointers(firstIt, secondIt, ret));
	}

	private boolean advancePointers(ListIterator<T> firstIt, ListIterator<T> secondIt, int ret) {
		//if values different, increment smaller one, unless its input is finished - then we're done too
		if (ret < 0) {
			if (firstIt.hasNext()) {
				firstValue = firstIt.next();
			}
			else 
				return false;
		}
		else if (ret > 0) {
			if (secondIt.hasNext())
				secondValue = secondIt.next();
			else
				return false;
		}
		return true;
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
		int cmpCnt = 0;
		@Override
		public int compare(DebugTuple v0, DebugTuple v1) {
			cmpCnt++;
			return v0.s.compareTo(v1.s);
		}
		
		public int lastCmpCnt() {
			int tmp = cmpCnt;
			cmpCnt = 0;
			return tmp;
		}
	}

	//
	public static class DebugTupleSortMerger extends SortMerger<DebugTuple> {
		ArrayList<String> result = new ArrayList<String>(); 
		DebugTupleComparator comparator = null;
		List<DebugTuple> first = null;
		List<DebugTuple> second = null;

		public DebugTupleSortMerger(List<DebugTuple> first, List<DebugTuple> second) {
			super(null);
			comparator = new DebugTupleComparator();
			super.comparator = comparator;
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
			return first.listIterator(positionIdx);
		}
		
		public int cmpCnt() {
			return comparator.lastCmpCnt();
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

		//same as above, but longer B group end of group == end of one input
		String c1[] = {"A", "B", "B", "B"};
		String d1[] = {"A", "B"};
		checkMergeOfArrays(c1, d1, 4);
		checkMergeOfArrays(d1, c1, 4);

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

		//I should learn how to run Java
		boolean handlesExceptions = false;
		try { assert(false); }
		catch (AssertionError err) {
			handlesExceptions = true;
		}
		if (! handlesExceptions) {
			System.err.println("ERROR: add -ea VM flags, to test for errors");
		}
		
		System.out.println("############################");
		System.out.println("TEST DONE!");

	}//main()

	private static void checkMergeOfArrays(String a[], String b[], int joinsNo) {
		List<DebugTuple> first = createTupleList(a);
		List<DebugTuple> second = createTupleList(b);	

		System.out.println("Merging: " + first);
		System.out.println("with   : " + second);

		DebugTupleSortMerger merger = new DebugTupleSortMerger(first, second);
		merger.doMerge(first.listIterator(), second.listIterator());
		List<String> result = merger.result();
		System.out.println("DONE: " + result + ": " + joinsNo + " ?= " + result.size() + " with comparisons " + merger.cmpCnt());
		assert(result.size() == joinsNo);
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