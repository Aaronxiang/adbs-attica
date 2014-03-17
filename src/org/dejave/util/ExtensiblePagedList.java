package org.dejave.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.dejave.attica.engine.operators.EngineException;
import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.BufferManager;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.PageIdentifier;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.Sizes;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIOManager;
import org.dejave.attica.storage.TupleIdentifier;

/**
 * Container for in-memory storage of tuples, backed up with a hard drive space. Unlike PagedArray it's extensible, meaning whenever there is a tuple added and already
 * all of list space is used, the array gets enlarged.
 * NOTE: In above case it gets enlarged only by 1 (unlike standard in-memory structures), since increasing the size is done with ::insertTuple() 
 * which forces page to be written to disk and we have no way to prevent that -> there is no need for the I/O penalty if we may not 
 * need more space in the future.
 * NOTE2: If we had access to the intermediate sorted files - no additional disk writing would be needed - I could iterate over the 
 * two files using SortMerger (first edition) and writing my own ListIterator<Tuple> (both directions iteration). However it seems
 * like too much of modifications to be done to the architecture of attica.
 * NOTE3: List capacity is increased if there is not enough space, but it's left so even after reset. However not big penalty is paid, since we use only one in-memory page 
 * at a time for accessing this file.
 * 
 * @author krzysztow
 *
 */
public class ExtensiblePagedList {
	//name of a file, that backs up the array on disk
	private String arrayFileName = null;
	//relation schema of tuples being inserted in array
	private Relation arrayFileRelation = null;
	//storage manager that accesses the file
	private StorageManager storageManager = null;
	//relation IO manager for the array file
	RelationIOManager arrayManager = null;

	//indicates number of tuples that fit into page
	private /*final*/ int tuplesNoPerPage = 0;

	//number of elements stored int a structure
	private int currentSize = 0;
	//possible number of elemenets stored in a structure
	private int currentCapacity = 0;
	//currently read page
	private Page currentPage = null;

	/**
	 * Creator of the class.
	 * @param tupleSize - size of tuples to be stored;
	 * @param pageSize - size of a single Page;
	 * @param rel - relation describing tuples stored in a page;
	 * @param sm - storage manager for a file;
	 * @param arrayFileName - name of a file backing the array.
	 * @throws EngineException
	 */
	public ExtensiblePagedList(int tupleSize, int pageSize, Relation rel, StorageManager sm, String arrayFileName) 
			throws EngineException {
		this.arrayFileName = arrayFileName;
		this.arrayFileRelation = rel;
		this.storageManager = sm;
		tuplesNoPerPage = pageSize / tupleSize;

		initArrayFile();
	}

	/**
	 * Called to put new tuple at the end of array. If there is not enough space, either new tuple is added to the last page of a file,
	 * or new page added to the file with tuple set.
	 * @param tuple to be added.
	 * @throws EngineException
	 */
	public void addTuple(Tuple tuple) throws EngineException {
		try {
			if (currentCapacity <= currentSize) {
				//if we filled the last page (or there was no page at all), insert tuple (which enlarges the file by page)
				//and then retrieve that page
				arrayManager.insertTuple(tuple);
				currentCapacity += tuplesNoPerPage;
				final int lastPageNo = pagePosForIdx(currentCapacity - 1);
				currentPage = storageManager.readPage(arrayFileRelation, new PageIdentifier(arrayFileName, lastPageNo));
			}
			else {
				//we are still within last page
				final int reqPagePos = pagePosForIdx(currentSize);
				//if we are on some other page, we need to switch to the desired one
				if (reqPagePos != currentPage.getPageIdentifier().getNumber()) {
					currentPage = storageManager.readPage(arrayFileRelation, new PageIdentifier(arrayFileName, reqPagePos));
				}

				//depending on if the tuple placeholder is already used or no, tither set or add tuple
				final int reqTuplePos = tuplePosForIdx(currentSize);
				if (reqTuplePos < currentPage.getNumberOfTuples()) {
					currentPage.setTuple(tuplePosForIdx(currentSize), tuple);				
				}
				else {
					assert (reqTuplePos == currentPage.getNumberOfTuples());
					currentPage.addTuple(tuple);
				}
			}
		} catch (StorageManagerException e) {
			throw new EngineException("Cannot add tuple to the paged list.", e);
		}

		++currentSize;
	}

	/**
	 * Returns index of a page where tuple of index idx exists;
	 */
	private int pagePosForIdx(int idx) {
		return idx / tuplesNoPerPage;
	}

	/**
	 * Returns index of tuple of index idx, within a page ({@link pagePosForIdx()}
	 */
	private int tuplePosForIdx(int idx) {
		return idx % tuplesNoPerPage;
	}

	/**
	 * Returns iterator over the {@link ExtensiblePagedList}
	 * NOTE: it does use another page (not the currentPage we do).
	 * @return
	 * @throws EngineException
	 */
	public Iterator<Tuple> iterator() throws EngineException {
		return new ExtensiblePagedArrayIterator();
	}

	private class ExtensiblePagedArrayIterator implements Iterator<Tuple> {
		int nextElement = 0;
		Iterator<Tuple> internalIt = null;

		public ExtensiblePagedArrayIterator() throws EngineException {
			this.nextElement = 0;
			try {
				this.internalIt = arrayManager.tuples().iterator();
			}
			catch (Exception e) {
				throw new EngineException("Cannot create iterator over the paged list.", e);
			}
		}

		@Override
		public boolean hasNext() {
			return (nextElement < ExtensiblePagedList.this.currentSize);
		}

		@Override
		public Tuple next() {
			nextElement++;
			return (internalIt.next());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Resets the array - from now on, size is zero and addValue() will start appending tuples from the beginning of the array.
	 * @throws EngineException
	 */
	public void reset() throws EngineException {
		currentSize = 0;
		try {
			Iterator<Page> pageIt = arrayManager.pages().iterator();
			if (pageIt.hasNext()) {
				currentPage = pageIt.next();
			}
			else {
				currentPage = null;
			}
		}
		catch (Exception e){
			throw new EngineException("Cannot reset the paged list.", e);
		}
	}

	private void initArrayFile() 
			throws EngineException {
		// create a temporary file to back-up heap memory
		try {
			storageManager.createFile(arrayFileName);
			arrayManager = new RelationIOManager(storageManager, arrayFileRelation, arrayFileName);
		} catch (StorageManagerException sme) {
			// remove the file that has just been created and re-throw
			try {storageManager.deleteFile(arrayFileName);} catch (Exception e) {};
			throw new EngineException("Could not instantiate extensible array backend.",
					sme);
		}
	}

	public void cleanup() 
			throws EngineException {
		try {
			storageManager.deleteFile(arrayFileName);
		} catch (StorageManagerException sme) {
			throw new EngineException("Could not clean up array file", sme);
		}
	}


	/*####################################################
	 *##### DEBUG
	 *####################################################
	 */

	public static void main(String args[]) 
			throws StorageManagerException, EngineException, IOException
			{
		BufferManager bm = new BufferManager(100);
		StorageManager sm = new StorageManager(null, bm);

		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(new Attribute("integer", Integer.class));
		attributes.add(new Attribute("string", String.class));
		Relation relation = new Relation(attributes);
		String filename = args[0];
		sm.createFile(filename);

		Tuple helpTuple = createNewConstantTuple(filename, 0);
		int tupleSize = TupleIOManager.byteSize(relation, helpTuple);
		int tuplesPerPage = Sizes.PAGE_SIZE / tupleSize;

		ExtensiblePagedList extArray = new ExtensiblePagedList(tupleSize, Sizes.PAGE_SIZE, relation, sm, filename);

		testExtArray(extArray, 3 * tuplesPerPage, filename);
		testExtArray(extArray, 1 * tuplesPerPage, filename);
		testExtArray(extArray, 4 * tuplesPerPage, filename);

		//I should learn how to run Java tests
		boolean handlesExceptions = false;
		try { assert(false); }
		catch (AssertionError err) {
			handlesExceptions = true;
		}
		if (! handlesExceptions) {
			System.err.println("ERROR: add -ea VM flags, to test for errors");
		}
			}

	private static void testExtArray(ExtensiblePagedList array, int tuplesNo, String filename) throws EngineException {
		Random r = new Random();
		r.setSeed(System.currentTimeMillis());

		Integer i0 = r.nextInt(1024);

		array.reset();

		System.out.println("Inserting " + tuplesNo + " elements...");
		HashMap<Integer, Tuple> checkHash = new HashMap<Integer, Tuple>();
		for (int i = i0; i < tuplesNo + i0; ++i) {
			Tuple t = createNewConstantTuple(filename, i);
			array.addTuple(t);
			checkHash.put(i, t);
			System.out.print(".");
		}
		System.out.println();

		System.out.println("Reading " + tuplesNo + "elements...");
		Iterator<Tuple> it = array.iterator();
		int iterNo = 0;
		while (it.hasNext()) {
			Tuple t = it.next();
			Tuple t1 = checkHash.remove(t.asInt(0));
			assert(t1 == t);
			System.out.print(".");
			++iterNo;
		}
		System.out.println();
		System.out.println("Iterated " + iterNo + " times and missed " + checkHash.size() + " elements.");

		assert(0 == checkHash.size());
		assert(iterNo == tuplesNo);
	}

	private static int writeRelation(String filename, RelationIOManager manager, int tuplesToWrite)
			throws StorageManagerException {
		int tuplesWritten = 0;
		System.out.println("Inserting...");
		for (int i = 0; i < tuplesToWrite; i++) {
			Tuple tuple = createNewConstantTuple(filename, i);
			System.out.print(".");
			manager.insertTuple(tuple);
			++tuplesWritten;
		}

		System.out.println("Tuples successfully inserted.");
		return tuplesWritten;
	}

	private static Tuple createNewConstantTuple(String filename, int identifier) {
		List<Comparable> v = new ArrayList<Comparable>();
		v.add(new Integer(identifier));
		v.add(new String("a-constant-length-very-long-attribute-to-fit-just-few-in-a-page-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAAA"));
		Tuple tuple = new Tuple(new TupleIdentifier(filename, identifier), v);

		return tuple;
	}
}
