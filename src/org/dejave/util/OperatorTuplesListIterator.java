package org.dejave.util;

import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.dejave.attica.engine.operators.EndOfStreamTuple;
import org.dejave.attica.engine.operators.EngineException;
import org.dejave.attica.engine.operators.Operator;
import org.dejave.attica.storage.Tuple;

/**
 * Iterator-like wrapper to go over the tuples pulled from Operator.
 * It encapsulates Operator behaviour of possible return of null (discard) and EOF tuple (done).
 * 
 * @author krzys
 * 
 */
public class OperatorTuplesListIterator implements ListIterator<Tuple> {
	private Tuple lastTuple = null;
	private boolean hasNxt = false;
	private Operator op = null;
	int nextIdx = 0;

	public OperatorTuplesListIterator(Operator operator) throws EngineException {
		op = operator;
		// no worries, first tuple returned by next is null, we don't loose anything;
		next();
	}

	/**
	 * Indicates if there is new tuple in a stream.
	 * @return
	 */
	public boolean hasNext() {
		return hasNxt;
	}

	/**
	 * Polls for a next tuple and returns new result.
	 * @return
	 */
	public Tuple next() {
		Tuple t = lastTuple;
		// iterate until non-null tuple is gotten
		// !NOTE: why does the tuple may be null?
		try {
			while (true) {
				lastTuple = op.getNext();
				if (null != lastTuple) {
					hasNxt = !(lastTuple instanceof EndOfStreamTuple);
					break;
				}
			}
		} catch (EngineException e) {
			throw new NoSuchElementException("Cannot retrieve next tuple from operator.");
		}
		
		++nextIdx;
		return t;
	}

	/**
	 * Method added so that one can peek what value was last read on {@link OperatorTuplesIterator::next()} invocation.
	 * @return
	 */
	public Tuple peek() {
		return lastTuple;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Cannot remove tuple from operator.");
	}

	@Override
	public void add(Tuple t) {
		throw new UnsupportedOperationException("Cannot add tuple to operator.");
	}

	@Override
	public boolean hasPrevious() {
		throw new UnsupportedOperationException("Operator can't iterate backwards.");
	}

	@Override
	public int nextIndex() {
		return nextIdx;
	}

	@Override
	public Tuple previous() {
		throw new UnsupportedOperationException("Operator can't iterate backwards.");
	}

	@Override
	public int previousIndex() {
		return nextIdx - 1;
	}

	@Override
	public void set(Tuple arg0) {
		// TODO Auto-generated method stub
		
	}
}