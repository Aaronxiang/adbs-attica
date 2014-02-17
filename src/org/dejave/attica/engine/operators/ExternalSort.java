/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.swing.text.html.MinimalHTMLWriter;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.FileUtil;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.Sizes;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIOManager;
import org.dejave.util.MinListHeap;
import org.dejave.util.Pair;

import com.sun.org.apache.bcel.internal.generic.NEWARRAY;

/**
 * ExternalSort: Your implementation of sorting.
 * 
 * @author sviglas
 */
public class ExternalSort extends UnaryOperator {

	/**
	 * Class that takes care of comparing tuples.
	 * 
	 * @author krzys
	 * 
	 */
	private static class TupleComparator implements Comparator<Tuple> {
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

	/**
	 * Don't implement Iterator, since every next() could throw exception, which
	 * iterators don't handle
	 * 
	 * @author krzys
	 * 
	 */
	private static class OperatorTuplesIterator /* implements Iterator<Tuple> */{
		private Tuple lastTuple = null;
		private boolean hasNxt = false;
		private Operator op = null;

		public OperatorTuplesIterator(Operator operator) throws EngineException {
			op = operator;
			// no worries, first tuple returned by next is null
			next();
		}

		public boolean hasNext() {
			return hasNxt;
		}

		public Tuple next() throws EngineException {
			Tuple t = lastTuple;
			// iterate until non-null tuple is gotten
			// !NOTE: why does the tuple may be null?
			while (true) {
				lastTuple = op.getNext();
				if (null != lastTuple) {
					hasNxt = !(lastTuple instanceof EndOfStreamTuple);
					break;
				}
			}
			return t;
		}

		public Tuple peek() {
			return lastTuple;
		}
	}

	public static class ArrayCreationResult {
		int elemsNoRead = 0;
		PagedArray array = null;

		public ArrayCreationResult(int elemsNoRead, PagedArray array) {
			this.elemsNoRead = elemsNoRead;
			this.array = array;
		}
	};

	public static ArrayCreationResult createPagedArray(int tupleSize,
			int pageSize, int maxPagesNo, Relation rel, StorageManager sm,
			String arrayFileName, OperatorTuplesIterator inOpIter)
					throws EngineException {
		int tuplesNoPerPage = pageSize / tupleSize;
		int maxArrayCapacity = tuplesNoPerPage * maxPagesNo;

		// create a temporary file to back-up heap memory
		RelationIOManager arrayManager = null;
		try {
			sm.createFile(arrayFileName);
			arrayManager = new RelationIOManager(sm, rel, arrayFileName);
		} catch (StorageManagerException sme) {
			// remove the file that has just been created and re-throw
			try {sm.deleteFile(arrayFileName);} catch (Exception e) {};
			throw new EngineException("Could not instantiate " + "heap-file",
					sme);
		}

		int arraySize = 0;
		Page pages[] = null;
		try {
			// materialize the heap file with first heapCapacity number of
			// tuples
			while (inOpIter.hasNext()) {//TODO: problem with last element
				arrayManager.insertTuple(inOpIter.next());
				arraySize++;
				// can't read more than fits into the allowed buffer size
				if (arraySize >= maxArrayCapacity)
					break;
			}

			// claim (store references of) all necessary pages
			// it handles the case, when there was less tuples read than memory
			// available - the array will just use less pages than provided
			Iterator<Page> pagesIt = arrayManager.pages().iterator();
			int realPagesNo = (int) Math
					.ceil(1.0 * arraySize / tuplesNoPerPage);
			pages = new Page[realPagesNo];
			for (int i = 0; i < realPagesNo; ++i) {
				pages[i] = pagesIt.next();
			}
		} catch (Exception sme) {
			throw new EngineException("Couldn't initialize the array file", sme);
		}

		return new ArrayCreationResult(arraySize, new PagedArray(
				tuplesNoPerPage, arraySize * tuplesNoPerPage, pages));
	}

	/**
	 * Call this method before reference to this class is dropped. The temporary
	 * file is deleted.
	 * 
	 * @throws EngineException
	 */
	public void cleanupPagedArray(String fileName) throws EngineException {
		try {
			sm.deleteFile(fileName);
		} catch (StorageManagerException sme) {
			throw new EngineException("Could not clean up array file", sme);
		}
	}

	private static class PagedArray extends AbstractList<Tuple> {
		// calculated number of tuples per page
		int tuplesNoPerPage = 0;
		// number of tuples that may be stored in the buffer created
		int arrayCapacity = 0;
		// stores the filename of the materialized array structure
		// String arrayFile = null;
		// keeps pointers to Pages that constitute to this array
		Page pages[] = null;

		public PagedArray(int tuplesNoPerPage, int arrayCapacity,
				Page pages[]) throws EngineException {
			this.tuplesNoPerPage = tuplesNoPerPage;
			this.arrayCapacity = arrayCapacity;
			this.pages = pages;
		}

		/**
		 * Returns index of a page where tuple of index idx exists;
		 */
		private int pagePosForIdx(int idx) {
			return idx / tuplesNoPerPage;
		}

		/**
		 * Returns index of tuple of index idx, within a page ({@link
		 * pagePosForIdx()}
		 */
		private int tuplePosForIdx(int idx) {
			return idx % tuplesNoPerPage;
		}

		@Override
		public Tuple get(int idx) {
			if (idx < 0 || idx >= size()) {
				throw new IndexOutOfBoundsException();
			}
			Page page = pages[pagePosForIdx(idx)];
			return page.retrieveTuple(tuplePosForIdx(idx));
		}

		@Override
		public Tuple set(int idx, Tuple t) {
			if (idx < 0 || idx >= size()) {
				throw new IndexOutOfBoundsException();
			}
			Page page = pages[pagePosForIdx(idx)];
			Tuple replaced = page.retrieveTuple(tuplePosForIdx(idx));
			page.setTuple(tuplePosForIdx(idx), t);
			return replaced;
		}

		/**
		 * Removes element at the position idx, setting it to null. (AbstractList::remove()
		 * name changed deliberately, since array cell is not removed).
		 * NOTE: method disabled, since we can't use set(idx, null)
		 */
		/*
		 * public Tuple removeAt(int idx) {
			if (idx < size()) {// has to be less than the size
				Tuple t = get(idx);
				//set(idx, null);//you can't nullify the tuple, since canSubstitute() segfaults.
				return t;
			}
			throw new IndexOutOfBoundsException();
		}
		*/

		/**
		 * Returns total number of elements that can be stored in this array.
		 */
		@Override
		public int size() {
			return arrayCapacity;
		}
	}

	/**
	 * Helper class used for storing data over the file to be merged.
	 * It handles sequential read from the merge file with use of attica classes.
	 * @author krzys
	 */
	static class MergeFilesData {
		private RelationIOManager rioManager = null;
		private Iterator<Tuple> tuplesIt = null;
		private String fileName = null;
		private Tuple currentValue = null;

		/**
		 * C'tor.
		 * @param fileName - name of the file that contains run tuples;
		 * @param rel - relation which describes the file;
		 * @param sm - storage manager - this is reused so that we can make use of buffered pages.
		 * @throws IOException
		 * @throws StorageManagerException
		 */
		public MergeFilesData(String fileName, Relation rel, StorageManager sm) 
				throws IOException, StorageManagerException {
			this.rioManager = new RelationIOManager(sm, rel, fileName);
			this.tuplesIt = rioManager.tuples().iterator();
			this.fileName = fileName;
			if (tuplesIt.hasNext()) {
				currentValue = tuplesIt.next();
			}
		}

		/**
		 * Removes relation file for the given instance merge file.
		 * @param sm reference to StorageManager the instance was created with {@link MergeFilesData}
		 * @throws StorageManagerException propagated from StorageManager::deleteFile().
		 */
		public void removeFile(StorageManager sm) 
				throws StorageManagerException {
			sm.deleteFile(fileName);
		}

		/**
		 * @return current value pointed at the iterator.
		 */
		public Tuple value() {
			return currentValue;
		}

		/**
		 * Gets next value (if exists) from the iterator and sets it as current value.
		 * @return next value, if iterator has ended then null.
		 */
		public Tuple nextValue() {
			if (tuplesIt.hasNext()) {
				currentValue = tuplesIt.next();
			}
			else {
				currentValue = null;
			}
			return currentValue;
		}
	}

	/**
	 * Helper class used for comparison of MergeFilesData.
	 * It is used for building (and heapifying) a heap. Only takes care of the value of current tuple.
	 * @author krzys
	 *
	 */
	static class MFDComparator implements Comparator<MergeFilesData> {
		TupleComparator comparator = null;

		public MFDComparator(TupleComparator comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(MergeFilesData v1, MergeFilesData v2) {
			return comparator.compare(v1.value(), v1.value());
		}
	}

	/** The storage manager for this operator. */
	private StorageManager sm;

	/** The name of the temporary file for the output. */
	private String outputFile;

	/** The manager that undertakes output relation I/O. */
	private RelationIOManager outputMan;

	/** The slots that act as the sort keys. */
	private int[] slots;

	/**
	 * Number of buffers (i.e., buffer pool pages and output files).
	 */
	private int buffers;

	/** Iterator over the output file. */
	private Iterator<Tuple> outputTuples;

	/** Reusable tuple list for returns. */
	private List<Tuple> returnList;

	/**
	 * Constructs a new external sort operator.
	 * 
	 * @param operator
	 *            the input operator.
	 * @param sm
	 *            the storage manager.
	 * @param slots
	 *            the indexes of the sort keys.
	 * @param buffers
	 *            the number of buffers (i.e., run files) to be used for the
	 *            sort.
	 * @throws EngineException
	 *             thrown whenever the sort operator cannot be properly
	 *             initialized.
	 */
	public ExternalSort(Operator operator, StorageManager sm, int[] slots,
			int buffers) throws EngineException {

		super(operator);
		this.sm = sm;
		this.slots = slots;
		this.buffers = buffers;
		try {
			// create the temporary output files
			initTempFiles();
		} catch (StorageManagerException sme) {
			throw new EngineException("Could not instantiate external sort",
					sme);
		}
	} // ExternalSort()

	/**
	 * Initializes the temporary files, according to the number of buffers.
	 * 
	 * @throws StorageManagerException
	 *             thrown whenever the temporary files cannot be initialized.
	 */
	protected void initTempFiles() throws StorageManagerException {
		// //////////////////////////////////////////
		//
		// initialise the temporary files here
		// make sure you throw the right exception
		// in the event of an error
		//
		// for the time being, the only file we
		// know of is the output file
		//
		// //////////////////////////////////////////
		outputFile = FileUtil.createTempFileName();
        sm.createFile(outputFile);
	} // initTempFiles()

	/**
	 * Sets up this external sort operator.
	 * 
	 * @throws EngineException
	 *             thrown whenever there is something wrong with setting this
	 *             operator up
	 */
	public void setup() throws EngineException {
		returnList = new ArrayList<Tuple>();
		try {
			// //////////////////////////////////////////
			//
			// this is a blocking operator -- store the input
			// in a temporary file and sort the file
			//
			// //////////////////////////////////////////

			// //////////////////////////////////////////
			//
			// YOUR CODE GOES HERE

			/*
			 * input operator - it can be anything;
			 * storage manager - link to the system for io, don't need to modify it
			 * slots array - array of indexes into attributes of input.  It could look like slots = {2, 3, 0} - contain
			 * indexes to field # of record - primary sort key - attribute 2. If two values of attribute 2 are compared and are equal, move
			 * to number 3. If still the same, move to 0 slot.
			 * buffers - B pages for sorting -> buffers - 1 is our memory
			 * 
			 * How to access atributoe of tuple -? for every type there is a method that goes to slot value and casts it to appropriate type. 
			 * e.g. asInt(slot) - takes bytes and casts as int.
			 * Simpler!!!: getValue() - comparable, which allows to call compareTo() method. Makes sense to call it, since no type problems will be there. Same
			 * schema - and we access same slots - the types will be compatible. Output of compare 0 - equal, +1 if greater. 
			 * 
			 * Dont worry about the schema or types. 
			 * For loop for slots array, if can prematrely decicde - break;
			 * 
			 * 
			 * Generate runs:
			 * 1) Open page iterator over input, read B pages and sort -> how to sort - don't take single page and call collection sort - thats bad. It's already
			 * in the main memory. 
			 * 2) or we can assume, input is of same, fixed size - compute, how many records fit in a single pafge - TupleIO::byteSize(). If page size has 
			 * 4kB and tuple same size - get number of tuples. So we know how many tuples there will be in a heap. Java Queue - make sure don't exceed. 
			 * Do Replacement-Selection!!!!! 
			 * 
			 * Code has to be:
			 * - readable;
			 * - no necessary copies, wierd things;
			 * - code quality;
			 * - efficiency;
			 * 
			 * How many pages at the same time?
			 * How to merge B-pages at the same time?
			 * 
			 * Submit: source code and complied version of implementation - attica.jar - because scripts are used.
			 * Setup example output and queries. But test also with large inputs - 1000 000 inputs. Test, where number of pages are not enough to store input.
			 * 1000, 10 000 and 1000 000.
			 * 1, 3 and 5 keys.
			 * 
			 */

			/* iteartor, write it to output and advance only this one
			 * 
			 * //instaad to array - create a heap
			 * 
			 * //delete B file names from the beginning of the list } }
			 * 
			 * 
			 * Not straightforward Array of pages - use the same technique of
			 * taking index and turning into offsets - turn into a heap -
			 * replacement selection //keep a counter that acts as a division
			 * point between two heaps
			 * 
			 * Another implementation - priority queues for queues but ensuring
			 * we don't exceed the N number Timings - if example schema used -
			 * sorting 1 million records, 50 buffer per pages, 1 sort -> 10 - 15
			 * seconds
			 * 
			 *  go through the algorithm
			 *  
			 *  Two versions - 
			 *  	1st - standar extern merge-sort;
			 *  	2nd - replacement selection;
			 *  
			 *  Straightforward version of the algorithm:
			 *  - open that file, access it via pages(). At some time there will be continous access to pages. Length of array
			 *  should be as many as pages;
			 *  
			 *  int B = buffers;
			 *  Page pages[] = new Pages[B];
			 *  
			 *  man = new RelationIOManager();
			 *  int counter = 0;
			 *  for (Page p : man.pages()) {
			 *  	pages[counter++] = p;
			 *  	if (counter == B) {//all pages that fit in buffer are read
			 *  		//sort pages [0..B-1]
			 *  		int rpp = Page.SIZE / TupleIOManager.buteSize(pages[0].getTuple[0]);//no of records per page
			 *  		int N = B * rpp;//no of tuples that are read into memory
			 *  		
			 *  		//access the array
			 *  		int pageOffset = j / rpp;
			 *  		int tupleOffset = j % rpp;
			 *  	
			 *  		//apply main memory sort
			 *  
			 *  		//keep track of files
			 *  		listOfFiles.add(FileUtils.createTempFile());
			 *  		RelationalIOManager relMan = 
			 *  			new RelationalIOManager(getStorageManager(), rel, listOfFiles.get(listOfFiles.size() - 1);
			 *  		//iterate over sorted array and output them
			 *  		relMan.insertTuples(...);
			 *  
			 *  		counter = 0;
			 *  	}
			 *  	//ended up with sorted runs on disk and list of fileNames with all those runs
			 *  
			 *  	//merge runs
			 *  	//if we merge fewer thatn B runs, it's a final run
			 *  	
			 *  	while (listOfFiles.size() > B) {//or !listOfFiles.empty()
			 *  		if (listOfFileNames.szie() < B) {//final output
			 *  			RelationalIOManager relMan = 
			 *  				new RelationalIOManager(getStorageManager(), ..., outputFile);
			 *  		}
			 *  		else {//another run
			 *  			String tmpFileName = FileUtils.createTempFileName();
			 *  			runs.add(tmpFileName);
			 *  			RelationalIOManager relMan = 
			 *  				new RelationalIOManager(getStorageManager(), ..., tmpFileName);
			 *  			
			 * 			
			 *  		}
			 *  
			 *  		//instantiate as many relIOmanagers as files to merge
			 *  		//have B iterators over them, tuples
			 *  		//read tuples
			 *  		//find minimum iteartor, write it to output and advance only this one
			 *  
			 *  		//instaad to array - create a heap
			 *  
			 *  		//delete B file names from the beginning of the list
			 *  	}
			 *  }
			 *  
			 *  
			 *  Not straightforward
			 *  Array of pages - use the same technique of taking index and turning into offsets - turn into a heap - replacement selection
			 *  //keep a counter that acts as a division point between two heaps
			 *  
			 *  Another implementation - priority queues for queues but ensuring we don't exceed the N number
			 *  Timings - if example schema used - sorting 1 million records, 50 buffer per pages, 1 sort -> 10 - 15 seconds
			 *  
			 */
			// //////////////////////////////////////////

			// //////////////////////////////////////////
			//
			// the output should reside in the output file
			//
			// //////////////////////////////////////////

			if (false) {

				Relation rel = getInputOperator().getOutputRelation();
				RelationIOManager rMan =
						new RelationIOManager(sm, rel, outputFile);
				boolean done = false;
				while (! done) {
					Tuple tuple = getInputOperator().getNext();
					if (tuple != null) {
						done = (tuple instanceof EndOfStreamTuple);
						if (! done) rMan.insertTuple(tuple);
					}
				}
				
				outputTuples = rMan.tuples().iterator();
			}
			else {

				Operator inOperator = getInputOperator();
				Relation inRelation = inOperator.getOutputRelation();
				OperatorTuplesIterator opTupIter = new OperatorTuplesIterator(inOperator);

				if (opTupIter.hasNext()) {
					String arrayBufferFile = FileUtil.createTempFileName();
					//buffers - 2 is used, since we use 1 page for input and 1 for output
					ArrayCreationResult res = createPagedArray(TupleIOManager.byteSize(inRelation, opTupIter.peek()), 
							Sizes.PAGE_SIZE, buffers - 2, inRelation, sm, arrayBufferFile, opTupIter);

					PagedArray buffArray = res.array;
					//number of elements that count into current run (they are heapified)
					//these elements take positions [0..heapElementsNo-1] in the array
					int heapElementsNo = res.elemsNoRead;
					//start of the next run partial array
					//these elements take positions [nextRunStartIdx..buffArray.size()-1]
					int nextRunStartIdx = buffArray.size();

					TupleComparator comparator = new TupleComparator(slots);
					MinListHeap<Tuple> heapifier = new MinListHeap<Tuple>();
					heapifier.buildHeap(buffArray, heapElementsNo, comparator);

					ArrayList<String> runFiles = new ArrayList<String>();

					//generate run files
					while (true) {
						//new run file
						String runFile = FileUtil.createTempFileName();
						runFiles.add(runFile);
						sm.createFile(runFile);
						RelationIOManager runRIOMgr = new RelationIOManager(sm, inRelation, runFile);

						Tuple lastOutTuple = null;//last tuple that was output to the output page
						Tuple newInTuple = null;//tuple taken from the input page
						while (0 != heapElementsNo) {//while current run is ongoing
							//take minimum element and output to run file
							lastOutTuple = buffArray.get(0);
							runRIOMgr.insertTuple(lastOutTuple);
							
							if (opTupIter.hasNext()) 
								newInTuple = opTupIter.next();

							//if there is another input tuple, read it
							if (null != newInTuple) {//NOTE: can we do this with hasNext() instead of checking for null?!
								int cmpRet = comparator.compare(lastOutTuple, newInTuple);
								if (cmpRet > 0) {//new tuple is smaller, put to the next run
									nextRunStartIdx--;
									heapElementsNo--;
									//evicted tuple will never be null - since we keep the array full
									Tuple evictedTuple = buffArray.set(nextRunStartIdx, newInTuple);
									buffArray.set(0, evictedTuple);
								}
								else {//tuple bigger, could be used in a current run
									buffArray.set(0, newInTuple);
								}							
							}
							else {//no more input
								Tuple lastHeapTuple = buffArray.get(heapElementsNo - 1);
								buffArray.set(0, lastHeapTuple);
								--heapElementsNo;
							}

							//preserve heap property
							heapifier.heapify(buffArray, 0, heapElementsNo, comparator);
						}

						if (buffArray.size() == nextRunStartIdx) {//no more input, no elements for a next run
							break;
						}
						else {
							if (0 == nextRunStartIdx) {//next run fills entire buffer
								heapElementsNo = buffArray.size();
								nextRunStartIdx = buffArray.size();
							}
							else {//if doesn't - that means we are run out of input. Move elements to the beginning of the array
								heapElementsNo = 0;
								for (; nextRunStartIdx != buffArray.size(); ++nextRunStartIdx, ++heapElementsNo) {
									buffArray.set(heapElementsNo, buffArray.get(nextRunStartIdx));
								}
							}
							//build a new heap
							heapifier.buildHeap(buffArray, heapElementsNo, comparator);
						}
					}

					//merge run files
					ArrayList<String> currentRunFiles = runFiles;
					ArrayList<String> newRunFiles = new ArrayList<String>();	
					RelationIOManager runFileRelManager = null;
					final int buffersNoForMerge = buffers - 1;//there is one output page and others may be used for input runs
					ArrayList<MergeFilesData> mergedRuns = new ArrayList<ExternalSort.MergeFilesData>();
					MFDComparator mfdComparator = new MFDComparator(comparator);
					MinListHeap<MergeFilesData> mfdHeapifier = new MinListHeap<ExternalSort.MergeFilesData>();

					//if we are not done yet with merging, keep going
					while (currentRunFiles.size() > 0) {
						if (currentRunFiles.size() < buffersNoForMerge) {//this is a final merge
							runFileRelManager = 
									new RelationIOManager(sm, inRelation, outputFile);
						}

						while (0 != currentRunFiles.size()) {
							if (null == runFileRelManager) {//this is not the final merge, so add temporary file
								String runFileName = FileUtil.createTempFileName();
								newRunFiles.add(runFileName);
								runFileRelManager = 
										new RelationIOManager(sm, inRelation, runFileName);
								newRunFiles.add(runFileName);
							}

							int mergedRunsNo = Math.min(currentRunFiles.size(), buffersNoForMerge);
							for (int i = 0; i < mergedRunsNo; ++i) {
								mergedRuns.add(new MergeFilesData(currentRunFiles.remove(i), inRelation, sm));
							}

							mfdHeapifier.buildHeap(mergedRuns, mfdComparator);
							while (! mergedRuns.isEmpty()) {
								//take heap root (minimum element) and output it
								MergeFilesData d = mergedRuns.get(0);
								runFileRelManager.insertTuple(d.value());
								if (null == d.nextValue()) {//if this was the last tuple from this run, remove from list
									d.removeFile(sm);//clean after yourself
									d = mergedRuns.remove(mergedRunsNo - 1);
									if (! mergedRuns.isEmpty())
										mergedRuns.set(0, d);
								}
								mfdHeapifier.heapify(mergedRuns, 0, mfdComparator);
							}
						}

						currentRunFiles.addAll(newRunFiles);
						newRunFiles.clear();
					}


					cleanupPagedArray(arrayBufferFile);
					outputTuples = runFileRelManager.tuples().iterator();
				}

			}

		} catch (Exception sme) {
			throw new EngineException("Could not store and sort"
					+ "intermediate files.", sme);
		}
	} // setup()

	/**
	 * Cleanup after the sort.
	 * 
	 * @throws EngineException
	 *             whenever the operator cannot clean up after itself.
	 */
	public void cleanup() throws EngineException {
		try {
			// //////////////////////////////////////////
			//
			// make sure you delete the intermediate
			// files after sorting is done
			//
			// //////////////////////////////////////////

			// //////////////////////////////////////////
			//
			// right now, only the output file is
			// deleted
			//
			// //////////////////////////////////////////
			sm.deleteFile(outputFile);
		} catch (StorageManagerException sme) {
			throw new EngineException("Could not clean up final output.", sme);
		}
	} // cleanup()

	/**
	 * The inner method to retrieve tuples.
	 * 
	 * @return the newly retrieved tuples.
	 * @throws EngineException
	 *             thrown whenever the next iteration is not possible.
	 */
	protected List<Tuple> innerGetNext() throws EngineException {
		try {
			returnList.clear();
			if (outputTuples.hasNext())
				returnList.add(outputTuples.next());
			else
				returnList.add(new EndOfStreamTuple());
			return returnList;
		} catch (Exception sme) {
			throw new EngineException("Could not read tuples "
					+ "from intermediate file.", sme);
		}
	} // innerGetNext()

	/**
	 * Operator class abstract interface -- never called.
	 */
	protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
			throws EngineException {
		return new ArrayList<Tuple>();
	} // innerProcessTuple()

	/**
	 * Operator class abstract interface -- sets the ouput relation of this sort
	 * operator.
	 * 
	 * @return this operator's output relation.
	 * @throws EngineException
	 *             whenever the output relation of this operator cannot be set.
	 */
	protected Relation setOutputRelation() throws EngineException {
		return new Relation(getInputOperator().getOutputRelation());
	} // setOutputRelation()

} // ExternalSort
