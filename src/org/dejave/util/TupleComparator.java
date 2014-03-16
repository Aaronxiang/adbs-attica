package org.dejave.util;

import java.util.Comparator;

import org.dejave.attica.storage.Tuple;

/**
 * Helper class for comparing tuples given the slots (and slots) order to compare with.
 * @author krzys
 * 
 */
public class TupleComparator implements Comparator<Tuple> {
	private int[] slots;

	public TupleComparator(int[] slots) {
		this.slots = slots;
	}

	public int compare(Tuple first, Tuple second) {
		int ret = 0;
		for (int i = 0; i < slots.length; ++i) {
			ret = first.getValue(slots[i]).compareTo(
					second.getValue(slots[i]));
			if (0 != ret)
				break;
		}

		return ret;
	}
}