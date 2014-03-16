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
import org.dejave.util.OperatorTuplesIterator;
import org.dejave.util.TupleComparator;

/**
 * ExternalSort: Your implementation of sorting.
 * 
 * @author sviglas
 * 
 * Krzysztow: Code implements the heap-sort replacement selection algorithm with use of own Heap builder class.
 * It is divided into two steps:
 * - mergeRunFiles() - create run files out of the input - this is done with use of page-oriented array structure and heap builder class;
 * - createRunFiles() - merge the files using B-1 merge algorithm (as mentioned below, also modified version is presented but  
 */
public class ExternalSort extends UnaryOperator {

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
	 * @param operator the input operator.
	 * @param sm the storage manager.
	 * @param slots the indexes of the sort keys.
	 * @param buffers the number of buffers (i.e., run files) to be used for the sort.
	 * @throws EngineException thrown whenever the sort operator cannot be properly initialized.
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

			Operator inOperator = getInputOperator();
			Relation inRelation = inOperator.getOutputRelation();
			OperatorTuplesIterator opTupIter = new OperatorTuplesIterator(inOperator);

			if (opTupIter.hasNext()) {
				ArrayList<String> runFiles = createRunFiles(inRelation, opTupIter);
				outputMan = mergeRunFiles(inRelation, runFiles);

				outputTuples = outputMan.tuples().iterator();
			}
			else {
				//how can that happen?
			}

		} catch (Exception sme) {
			throw new EngineException("Could not execute selection replacement sort.", sme);
		}
	} // setup()



	/**
	 * Creates a bunch of temporary, sorted files out of the one source tuple iterator. 
	 * @param inRelation relation to which schema incomming tuples conform to;
	 * @param opTupIter iterator over input operator's tuples stream;
	 * @return list of the temporary run files.
	 * @throws EngineException
	 * @throws StorageManagerException
	 */
	private ArrayList<String> createRunFiles(Relation inRelation, OperatorTuplesIterator opTupIter) 
			throws EngineException, StorageManagerException {
		//create an in-memory array to sort runs
		//size = (buffers - 2), since we use 1 page for input and 1 for output
		String arrayBufferFile = FileUtil.createTempFileName();
		ArrayCreationResult res = createPagedArray(TupleIOManager.byteSize(inRelation, opTupIter.peek()), 
				Sizes.PAGE_SIZE, buffers - 2, inRelation, sm, arrayBufferFile, opTupIter);

		PagedArray buffArray = res.array;
		//number of elements that count into current run (they are heapified)
		//these elements take positions [0..heapElementsNo-1] in the array
		int heapElementsNo = res.elemsNoRead;
		//index of start of the next run array
		//these elements take positions [nextRunStartIdx..buffArray.size()-1]
		int nextRunStartIdx = buffArray.size();

		TupleComparator comparator = new TupleComparator(slots);
		MinListHeap<Tuple> heapifier = new MinListHeap<Tuple>();
		heapifier.buildHeap(buffArray, heapElementsNo, comparator);

		ArrayList<String> runFiles = new ArrayList<String>();

		//generate run files until we are run out of input
		while (true) {
			//new run file
			String runFile = FileUtil.createTempFileName();
			runFiles.add(runFile);
			sm.createFile(runFile);
			RelationIOManager runRIOMgr = new RelationIOManager(sm, inRelation, runFile);

			//last tuple that was output to the output page
			Tuple lastOutTuple = null;
			//tuple taken from the input page
			Tuple newInTuple = null;
			while (0 != heapElementsNo) {//while current run is ongoing
				//take minimum element and output to run file
				lastOutTuple = buffArray.get(0);
				runRIOMgr.insertTuple(lastOutTuple);

				//if there is another input tuple, read it
				if (opTupIter.hasNext()) {
					newInTuple = opTupIter.next();
					int cmpRet = comparator.compare(lastOutTuple, newInTuple);
					if (cmpRet > 0) {//new tuple is smaller, put to the next run
						nextRunStartIdx--;
						heapElementsNo--;
						//evicted tuple will never be null, since we keep the array full, so following is safe
						Tuple evictedTuple = buffArray.set(nextRunStartIdx, newInTuple);
						if (0 != nextRunStartIdx)
							buffArray.set(0, evictedTuple);
					}
					else {//tuple is larger, will be used in a current run
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
		cleanupPagedArray(arrayBufferFile);
		return runFiles;
	}

	/**
	 * Writes sorted sequence of tuples to the output file, given a list of run files of sorted tuples.
	 * 
	 * NOTE: There is another implementation below, mergeRunFiles2(), which tries to keep opened B-1 files all the time
	 * and make use of it, see description to it. However it doesn't improve much in general case (apart from cases when files are partially sorted).
	 * 
	 * @param inRelation relation to which schema all the files conform to;
	 * @param runFiles list of files to be merged.
	 * @return RelationIOManager reference to the output file.
	 * @throws IOException
	 * @throws StorageManagerException
	 */
	private RelationIOManager mergeRunFiles(Relation inRelation, ArrayList<String> runFiles)
			throws EngineException {
		RelationIOManager runFileRelManager = null;
		RelationIOManager outputRelManager = null;
		//files to be merged in a current iteration
		ArrayList<String> currentRunFiles = new ArrayList<String>();
		//files scheduled to merge in next iteration - they are a result of merging current run files
		ArrayList<String> newRunFiles = new ArrayList<String>();
		//there is one output page and other left may be used for input runs	
		final int buffersNoForMerge = buffers - 1;
		//currently merged files (B-1) - keeps track of tuples iterators and references to appropriate files
		ArrayList<MergeFilesData> mergedRuns = new ArrayList<ExternalSort.MergeFilesData>();
		MFDComparator mfdComparator = new MFDComparator(new TupleComparator(slots));
		MinListHeap<MergeFilesData> mfdHeapifier = new MinListHeap<ExternalSort.MergeFilesData>();

		newRunFiles = runFiles;
		
		try {

			//if we are not done yet with merging, keep going
			while (newRunFiles.size() > 0) {
				currentRunFiles.addAll(newRunFiles);
				newRunFiles.clear();
				
				if (currentRunFiles.size() <= buffersNoForMerge) {//if this is a final merge
					outputRelManager = 
							new RelationIOManager(sm, inRelation, outputFile);
					runFileRelManager = outputRelManager;
				}

				while (0 != currentRunFiles.size()) {//merge B-1 files simultaneously
					if (null == runFileRelManager) {//this is not the final merge, so add temporary file
						runFileRelManager = createRelationIOManager(inRelation);
						newRunFiles.add(runFileRelManager.getFileName());
					}

					//merge either B-1 files or less if there is less files left
					final int MergedRunsNo = Math.min(currentRunFiles.size(), buffersNoForMerge);
					for (int i = 0; i < MergedRunsNo; ++i) {
						mergedRuns.add(new MergeFilesData(currentRunFiles.remove(currentRunFiles.size() - 1), inRelation, sm));
					}

					mfdHeapifier.buildHeap(mergedRuns, mfdComparator);
					while (! mergedRuns.isEmpty()) {
						//take heap root (minimum element) and output it
						MergeFilesData d = mergedRuns.get(0);
						runFileRelManager.insertTuple(d.value());
						if (null == d.nextValue()) {//if this was the last tuple from this run, remove temporal file and its reference
							d.removeFile(sm);
							d = mergedRuns.remove(mergedRuns.size() - 1);
							if (0 < mergedRuns.size()) {
								mergedRuns.set(0, d);
							}
						}
						mfdHeapifier.heapify(mergedRuns, 0, mfdComparator);
					}
					runFileRelManager = null;
				}
			}
		} catch (Exception e) {
			//clean all the temporal files that got created here
			for (String f : newRunFiles) 
				try {sm.deleteFile(f);} catch (StorageManagerException ex) {}
			for (String f : currentRunFiles)
				try {sm.deleteFile(f);} catch (StorageManagerException ex) {}
			for (MergeFilesData d : mergedRuns) 
				try {d.removeFile(sm);} catch (StorageManagerException ex) {}
			
			throw new EngineException("Couldn't merge files.", e);
		}

		return outputRelManager;
	}

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
	 * @return this operator's output relation.
	 * @throws EngineException
	 *             whenever the output relation of this operator cannot be set.
	 */
	protected Relation setOutputRelation() throws EngineException {
		return new Relation(getInputOperator().getOutputRelation());
	} // setOutputRela

	/**
	 * Helper class to get results of @link createPagedArray() method.
	 * @author krzys
	 *
	 */
	public static class ArrayCreationResult {
		int elemsNoRead = 0;
		PagedArray array = null;

		public ArrayCreationResult(int elemsNoRead, PagedArray array) {
			this.elemsNoRead = elemsNoRead;
			this.array = array;
		}
	};

	/**
	 * Creates an array like structure backed up with attica paged file (arrayFileName).
	 * The array gets filled with maxPagesNo number of pages read from inOpIter (or less if there are not that many of them).
	 * @param tupleSize size of each tuple (in bytes) stored in the array to be created;
	 * @param pageSize size of the page in bytes;
	 * @param maxPagesNo maximum number of pages that the array may consist of;
	 * @param rel relation to which schema tuples conform to;
	 * @param sm storage manager reference;
	 * @param arrayFileName name of the file to which array tuples may be written to;
	 * @param inOpIter iterator over the input tuples;
	 * @return result of the array creation.
	 * @throws EngineException
	 */
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
			// materialize the heap file with first heapCapacity number of tuples
			while (inOpIter.hasNext()) {
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
			int realPagesNo = (int) Math.ceil(1.0 * arraySize / tuplesNoPerPage);
			pages = new Page[realPagesNo];
			for (int i = 0; i < realPagesNo; ++i) {
				pages[i] = pagesIt.next();
			}
		} catch (Exception sme) {
			throw new EngineException("Couldn't initialize the array file", sme);
		}

		//System.out.println("Tsize: " + tupleSize + ", pages " + maxPagesNo + ", realPages " + pages.length);
		
		return new ArrayCreationResult(arraySize, new PagedArray(
				tuplesNoPerPage, arraySize, pages));
	}

	/**
	 * Call this method before reference to this class is dropped. The temporary
	 * file gets deleted.
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

	/**
	 * Wrapper that enables for accessing tuples in in-memory array of Pages as if they were stored 
	 * in an array. 
	 * It extends AbstractList, so that it can be used with MinListHeap, but it doesn't shrinks or
	 * extends like lists.
	 * @author krzys
	 *
	 */
	private static class PagedArray extends AbstractList<Tuple> {
		// calculated number of tuples per page
		int tuplesNoPerPage = 0;
		// number of tuples that may be stored in the buffer created
		int arrayCapacity = 0;
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
		 * Returns index of tuple of index idx, within a page ({@link pagePosForIdx()}
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
			try {
				Page page = pages[pagePosForIdx(idx)];
				Tuple replaced = page.retrieveTuple(tuplePosForIdx(idx));
				page.setTuple(tuplePosForIdx(idx), t);
				return replaced;
			}
			catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("Idx: " + idx + " pFIdx(): " + pagePosForIdx(idx));
				throw new ArrayIndexOutOfBoundsException();
			}
		}

		/**
		 * Removes element at the position idx, setting it to null. (AbstractList::remove()
		 * name changed deliberately, since array cell is not removed).
		 * NOTE: method disabled, since we can't use set(idx, null) - Page::setTuple() doesn't like nulls, which is sensible.
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
	 * Helper class used for storing data over the file to be merged (merge part of the sorting algorithm).
	 * It handles sequential read from the merge file with use of attica classes.
	 * @author krzys
	 */
	static class MergeFilesData {
		//file name which this class is responsible for
		private String fileName = null;
		//points to the file 
		private RelationIOManager rioManager = null;
		private Iterator<Tuple> tuplesIt = null;
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
		
		@Override
		public String toString() {
			if (null == currentValue)
				return "NULL";
			return currentValue.toString();
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
			return comparator.compare(v1.value(), v2.value());
		}
	}
	
	
	
	/**
	 * Writes sorted sequence of tuples to the output file, given a list of run files of sorted tuples.
	 * This implementation tries to keep B-1 files opened all the time. If one opened file has ended, we open 
	 * a new one and check if it's smallest (first, since it's sorted) element is larger than the last output. If so, we can freely
	 * keep on adding tuples from this file. Otherwise they are left opened, and will be added for another run. 
	 * 
	 * Note: This implementation doesn't better than previous, generally, since newly added run file would have to have all elements larger
	 * than the one being replaced. With even tuples distribution, it will not be a case. 
	 *
	 * @param inRelation relation to which schema all the files conform to;
	 * @param runFiles list of files to be merged.
	 * @return RelationIOManager reference to the output file.
	 * @throws IOException
	 * @throws StorageManagerException
	 */
	private RelationIOManager mergeRunFiles2(Relation inRelation, ArrayList<String> runFiles)
			throws EngineException {
		RelationIOManager runFileRelManager = null;
		RelationIOManager outputRelManager = null;
		//files to be merged in a current iteration
		ArrayList<String> currentRunFiles = new ArrayList<String>();
		//files scheduled to be merged in next iteration - they are a result of merging current run files
		ArrayList<String> newRunFiles = new ArrayList<String>();	
		final int BuffersNoForMerge = buffers - 1;//there is one output page and others may be used for input runs
		
		//stores information of runs being merged
		ArrayList<MergeFilesData> mergedRuns = new ArrayList<ExternalSort.MergeFilesData>();
		//stores information of runs to be merged
		ArrayList<MergeFilesData> currentMergeOpenedFiles = new ArrayList<ExternalSort.MergeFilesData>();
		
		TupleComparator tupleComparator = new TupleComparator(slots);
		MFDComparator mfdComparator = new MFDComparator(tupleComparator);
		MinListHeap<MergeFilesData> mfdHeapifier = new MinListHeap<ExternalSort.MergeFilesData>();

		newRunFiles.addAll(runFiles);
		
		try {

			//if we are not done yet with merging, keep going
			while (! newRunFiles.isEmpty()) {
				currentRunFiles.addAll(newRunFiles);
				newRunFiles.clear();
				
				if (currentRunFiles.size() <= BuffersNoForMerge) {//this is a final merge
					outputRelManager = 
							new RelationIOManager(sm, inRelation, outputFile);
					runFileRelManager = outputRelManager;
				}
				else {
					runFileRelManager = createRelationIOManager(inRelation);
				}

				currentMergeOpenedFiles = new ArrayList<ExternalSort.MergeFilesData>();
				final int MergedRunsNo = Math.min(currentRunFiles.size(), BuffersNoForMerge);
				for (int i = 0; i < MergedRunsNo; ++i) {
					currentMergeOpenedFiles.add(new MergeFilesData(currentRunFiles.remove(currentRunFiles.size() - 1), inRelation, sm));
				}
				
				while (! currentMergeOpenedFiles.isEmpty()) {

					mergedRuns.addAll(currentMergeOpenedFiles);
					currentMergeOpenedFiles.clear();
					
					if (outputRelManager != runFileRelManager) {//this is not the final merge, so add temporary file
						runFileRelManager = createRelationIOManager(inRelation);
						newRunFiles.add(runFileRelManager.getFileName());
					}

					mfdHeapifier.buildHeap(mergedRuns, mfdComparator);
					while (! mergedRuns.isEmpty()) {
						//take heap root (minimum element) and output it
						MergeFilesData d = mergedRuns.get(0);
						Tuple lastOutTuple = d.value();
						runFileRelManager.insertTuple(lastOutTuple);
						if (null == d.nextValue()) {//if this was the last tuple from this run, remove temporal file and its reference
							d.removeFile(sm);
							d = mergedRuns.remove(mergedRuns.size() - 1);
							if (! mergedRuns.isEmpty())
								mergedRuns.set(0, d);
							//optimization - let's open another RelationalIOManager, since we have another spare page
							if (! currentRunFiles.isEmpty()) {
								MergeFilesData mfd = new MergeFilesData(currentRunFiles.remove(currentRunFiles.size() - 1), inRelation, sm);
								int cmpRet = tupleComparator.compare(lastOutTuple, mfd.value());
								//last written tuple was smaller (or equal) than the first of the given newly open run - we can add that run to current merging
								if (cmpRet <= 0) {
									mergedRuns.add(mfd);
								}
								else {
									currentMergeOpenedFiles.add(mfd);
								}
							}
						}
						mfdHeapifier.heapify(mergedRuns, 0, mfdComparator);
					}
				}
			}
		} catch (Exception e) {
			//clean all the temporal files that got created here
			for (String f : newRunFiles) 
				try {sm.deleteFile(f);} catch (StorageManagerException ex) {}
			for (String f : currentRunFiles)
				try {sm.deleteFile(f);} catch (StorageManagerException ex) {}
			for (MergeFilesData d : mergedRuns) 
				try {d.removeFile(sm);} catch (StorageManagerException ex) {}
			
			throw new EngineException("Couldn't merge files.", e);
		}

		return outputRelManager;
	}
	
	/**
	* Creates a RelationIOManager to a temporal file.
	*/
	private RelationIOManager createRelationIOManager(Relation inRelation)
			throws StorageManagerException {
		RelationIOManager runFileRelManager;
		String runFileName = FileUtil.createTempFileName();
		sm.createFile(runFileName);
		runFileRelManager = 
				new RelationIOManager(sm, inRelation, runFileName);
		return runFileRelManager;
	}

} // ExternalSort
