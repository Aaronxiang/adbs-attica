package org.dejave.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class MinListHeap<T> {	
	/**
	 * 
	 * @param list
	 * @param nodeIdx
	 * @param comparator
	 */
	public void heapify(AbstractList<T> list, int nodeIdx, Comparator<T> comparator) {
		heapify(list, list.size(), nodeIdx, comparator);
	}

	/**
	 * Creates minimum heap with iterative approach. (It could be also maximum, if the copmarator returned 
	 * reversed values for comparison).
	 * @param list - list to be heapified
	 * @param nodeIdx - node on which heapify property is to be mainained
	 * @param elementsCount - number of first list elements that should be arranged in a heap
	 * @param comparator
	 */
	public void heapify(AbstractList<T> list, int nodeIdx, int elementsCount, Comparator<T> comparator) {
		int leftChild, rightChild; 
		int extremeIdx = nodeIdx;

		while (true) {
			leftChild = leftChildIdx(extremeIdx);
			rightChild = rightChildIdx(extremeIdx);

			if (leftChild < elementsCount && //does the left child exist?
					(-1 == comparator.compare(list.get(leftChild), list.get(extremeIdx))) ) {//the leftChild node is greater(min-heap)/smaller(max-heap) than the parent
				extremeIdx = leftChild;
			}
			if (rightChild < elementsCount && //does the right child exist?
					(-1 == comparator.compare(list.get(rightChild), list.get(extremeIdx))) ) {//the rightChild node is greater(min-heap)/smaller(max-heap) than the extreme found earlier
				extremeIdx = rightChild;
			}

			if (extremeIdx != nodeIdx) {//the parent node didn't fulfill the max/min-heap property
				//swap places with the largest of the children
				T oldParentValue = list.set(nodeIdx, list.get(extremeIdx));
				list.set(extremeIdx, oldParentValue);
				
				nodeIdx = extremeIdx;
			}
			else
				break;
		}

	}

	public void buildHeap(AbstractList<T> list, Comparator<T> comparator) {
		buildHeap(list, list.size(), comparator);
	}

	/**
	 * 
	 * @param array
	 * @param elementsCount
	 * @param comparator
	 * @param algorithm
	 */
	public void buildHeap(AbstractList<T> list, int elementsCount, Comparator<T> comparator) {
		final int lastParentNodeIdx = elementsCount / 2 - 1;//the last child belongs to the last parent
		int currentIndex = lastParentNodeIdx;
		//invoke heapify on each element of every levels but last one, in a bottom-up manner
		for (; currentIndex >= 0; --currentIndex) {
			heapify(list, currentIndex, elementsCount, comparator);
		}
	}

	private void assertHeapProperty(AbstractList<T> list, int elementsCount, Comparator<T> copmarator) {
		final int lastParentNodeIdx = elementsCount / 2 - 1;//the last child belongs to the last parent
		int currentIndex = lastParentNodeIdx;
		//invoke heapify on each element of every levels but last one, in a bottom-up manner
		for (; currentIndex >= 0; --currentIndex) {
			T parentValue = list.get(currentIndex);
			int chIdx = leftChildIdx(currentIndex);
			if (chIdx < elementsCount)
				assert(1 != copmarator.compare(parentValue, list.get(chIdx)));
			chIdx = rightChildIdx(currentIndex);
			if (chIdx < elementsCount)
				assert(1 != copmarator.compare(parentValue, list.get(chIdx)));
		}
	}

	/**
	 * Returns parent index, given index of node in question
	 */
	public static int parentIdx(int nodeIdx) {
		return ((nodeIdx - 1) / 2);
	}

	/**
	 * Returns the height of the heap tree representation, given elements count.
	 * @param elementsCount
	 * @return
	 */
	public static int height(int elementsCount) {
		return (int)Math.ceil(Math.log10(elementsCount + 1)/Math.log10(2));
	}
	/**
	 * Returns index of the left child of node in question
	 */
	public static int leftChildIdx(int nodeIdx) {
		return (2 * nodeIdx + 1);
	}

	/**
	 * Returns index of the right child of node in question
	 */
	public static int rightChildIdx(int nodeIdx) {
		return (2 * nodeIdx + 2);
	}

	/**
	 * @param Tests
	 */
	public static void main(String[] args) {
		System.out.println("Starting...");
		
		Integer a[] = {1, 4, 2, 1, 5, 11, 12, 3, 4, 6};
		ArrayList<Integer> testArray = new ArrayList<Integer>(Arrays.asList(a));
		final int testedSize = a.length - 3;

		Comparator<Integer> copmarator = new Comparator<Integer>() {
			@Override
			public int compare(Integer arg0, Integer arg1) {
				return (arg0 == arg1 ? 0 : (arg0 < arg1 ? -1 : 1));
			}
		};
		
		MinListHeap<Integer> heap = new MinListHeap<Integer>();
		heap.buildHeap(testArray, testedSize, copmarator);
		
		heap.assertHeapProperty(testArray, testedSize, copmarator);
		
		System.out.println("Finished.");
	}

}
