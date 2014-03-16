package org.dejave.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.sun.org.apache.bcel.internal.generic.NEWARRAY;

public abstract class SortMerger2<T> {
	static abstract class MergerBuffer<T> {
		public abstract void begin();
		
		public abstract void storeValue(T value);
		
		public abstract Iterator<T> iterator();
	} 
	
	
	Comparator<T> comparator = null;
	MergerBuffer<T> buffer = null;
	T firstValue = null;
	T secondValue = null;
	
	/**
	 * Constructs new merger with a given comparator.
	 * @param comparator
	 */
	public SortMerger2(Comparator<T> comparator, MergerBuffer<T> buffer) {
		this.comparator = comparator;
		this.buffer = buffer;
	}

	/**
	 * Invoked when there are two values from different inputs to be merged.
	 * @param first - value of the first input;
	 * @param second - value of the second intput.
	 */
	public abstract void mergeValues(T first, T second);

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
				//values are same - merge them - begin merge
				mergeValues(firstValue, secondValue);
				
				buffer.begin();
				buffer.storeValue(firstValue);
				
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
						buffer.storeValue(firstValue);
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
							Iterator<T> fIt = buffer.iterator();
							while (fIt.hasNext()) {
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
	public static class DebugTupleSortMerger extends SortMerger2<DebugTuple> {
		static class DebugTupleMergerBuffer extends MergerBuffer<DebugTuple> {
			DebugTuple buffer[] = null;
			int size = 0;
			
			public DebugTupleMergerBuffer(int bufferSize) {
				buffer = new DebugTuple[bufferSize];
			}
			
			@Override
			public void begin() {
				size = 0;
			}

			private void increaseCapacityBy(int increaseSize) {
				System.out.println("Increased size from " + buffer.length + " to " + (buffer.length + increaseSize));
				DebugTuple newBuffer[] = new DebugTuple[buffer.length + increaseSize];
				for (int i = 0; i < buffer.length; ++i) {
					newBuffer[i] = buffer[i];
				}
				buffer = newBuffer;
			}
			
			@Override
			public void storeValue(DebugTuple value) {
				if (size >= buffer.length) {
					increaseCapacityBy(1);
				}
				buffer[size++] = value;
			}

			@Override
			public Iterator<DebugTuple> iterator() {
				return new Iterator<DebugTuple>() {
					int nextPos = 0;

					@Override
					public boolean hasNext() {
						return (nextPos < size);
					}

					@Override
					public DebugTuple next() {
						return buffer[nextPos++];
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException("I don't do that!");
					}
				};
			}
		}
		
		ArrayList<String> result = new ArrayList<String>(); 
		DebugTupleComparator comparator = null;
		List<DebugTuple> first = null;
		List<DebugTuple> second = null;

		public DebugTupleSortMerger(List<DebugTuple> first, List<DebugTuple> second, int internalBufferSize) {
			super(null, new DebugTupleMergerBuffer(internalBufferSize));
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
		
		public int cmpCnt() {
			return comparator.lastCmpCnt();
		}

	}

	/**
	 * Debug main.
	 */
	public static void main (String [] args) {
		int internalBuffersSize[] = {1, 100};
		
		//normal case
		String a[] = {"A", "A", "B", "B", "J", "J", "K"};
		String b[] = {"B", "C", "G", "H", "I", "I", "J", "J", "J", "M"};
		checkMergeOfArrays(a, b, 8, internalBuffersSize);
		checkMergeOfArrays(b, a, 8, internalBuffersSize);

		//end of group == end of one input
		String c[] = {"A", "B", "B"};
		String d[] = {"A", "B"};
		checkMergeOfArrays(c, d, 3, internalBuffersSize);
		checkMergeOfArrays(d, c, 3, internalBuffersSize);

		//same as above, but longer B group end of group == end of one input
		String c1[] = {"A", "B", "B", "B"};
		String d1[] = {"A", "B"};
		checkMergeOfArrays(c1, d1, 4, internalBuffersSize);
		checkMergeOfArrays(d1, c1, 4, internalBuffersSize);

		//beginning/end of group == beginning/end of input; one input takes all the array
		String e[] = {"J", "J", "K"};
		String f[] = {"J", "J", "J"};
		checkMergeOfArrays(e, f, 6, internalBuffersSize);
		checkMergeOfArrays(f, e, 6, internalBuffersSize);

		//beginning of group == beginning of input
		String g[] = {"A", "A", "B"};
		String h[] = {"A", "B"};
		checkMergeOfArrays(g, h, 3, internalBuffersSize);
		checkMergeOfArrays(h, g, 3, internalBuffersSize);

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

	private static void checkMergeOfArrays(String a[], String b[], int joinsNo, int internalBufferSizes[]) {
		List<DebugTuple> first = createTupleList(a);
		List<DebugTuple> second = createTupleList(b);	

		System.out.println("Merging: " + first);
		System.out.println("with   : " + second);

		for (int i = 0; i < internalBufferSizes.length; ++i) {
			DebugTupleSortMerger merger = new DebugTupleSortMerger(first, second, internalBufferSizes[i]);
			merger.doMerge(first.listIterator(), second.listIterator());
			List<String> result = merger.result();
			System.out.println("DONE: " + result + ": buffs no:" + internalBufferSizes[i] + ", comp. no: " + merger.cmpCnt() + ", joins no:" + joinsNo + " ?= " + result.size());
			assert(result.size() == joinsNo);
			System.out.println("");
		}
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