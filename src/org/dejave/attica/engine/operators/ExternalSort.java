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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.FileUtil;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIdentifier;

/**
 * ExternalSort: Your implementation of sorting.
 *
 * @author sviglas
 */
public class ExternalSort extends UnaryOperator {
	private static class TupleComparator {
		private int [] slots;
		
		public TupleComparator(int [] slots) {
			this.slots = slots;
		}
		
		public int compare(Tuple first, Tuple second) {
			for (int i = 0; i < slots.length; ++i) {
				int ret = first.getValue(slots[i]).compareTo(second.getValue(slots[i]));
				if ()
			}
			
			return 
		}
	}
	
	private class PagedHeap {
		int tuplesNoPerPage = 0;
		int heapCapacity = 0;
		int heapSize = 0;
		String heapFile = null;
		RelationIOManager heapMan = null;
		Page pages[] = null;

		public PagedHeap(int tupleSize, int pageSize, int pagesNo, Relation rel)
				throws EngineException {
			//calculate number of tuples per each page
			tuplesNoPerPage = pageSize / tupleSize;
			assert(tuplesNoPerPage > 0);
			//set the heap size
			heapCapacity = tuplesNoPerPage * pagesNo;

			//create a temporary file to back-up heap memory
			try {
				heapFile = FileUtil.createTempFileName();
				sm.createFile(heapFile);
				heapMan = new RelationIOManager(sm, rel, heapFile);
			}
			catch (StorageManagerException sme) {
				throw new EngineException("Could not instantiate "
						+ "heap-file", sme);
			}
		}

		public void populate(Iterable<Tuple> tuples) 
				throws EngineException {			
			try {
				//materialize the heap file with first heapCapacity number of tuples
				heapSize = 0;
				for (Tuple inTuple : tuples) {
					heapMan.insertTuple(inTuple);
					heapSize++;
					//we can't read more than fits into the buffer
					if (heapSize >= heapCapacity) 
						break;
				}
				
				//claim all necessary pages, it handles the case, when there was less tuples 
				//than memory available - heap will just not use all available pages
				Iterator<Page> pagesIt = heapMan.pages().iterator();
				int realPagesNo = (int)Math.ceil(1.0 * heapSize / tuplesNoPerPage);
				pages = new Page[realPagesNo];
				for (int i = 0; i < realPagesNo; ++i) {
					pages[i] = pagesIt.next();
				}
			}
			catch (Exception sme) {
				throw new EngineException("Couldn't initialize the heap-file", sme);
			}
		}
		
		public void buildHeap() {
		}
		
		public void heapify(int tupleIndex, TupleComparator comparator) {
			int left = leftChildIdx(tupleIndex);
			int rightTupleIdx = rightChildIdx(tupleIndex);
			int extremeIdx = tupleIndex;
			if (leftTupleIdx < heapSize && 
					comparator.compare(tupleAt(leftTupleIdx), tupleAt(extremeIdx)) {
				extremeIdx = leftTupleIdx;
			}
			if (rightTupleIdx < heapSize &&
					comparator.compare(tupleAt(rightTupleIdx), tupleAt(extremeIdx)) {
				extremeIdx = rightTupleIdx;
			}
			
			if (extremeIdx != tupleIndex) {//the parent node didn't fulfill the max/min-heap property - go deeper
				//swap places with the largest of the children
				swapTuples(extremeIdx, tupleIndex);
				
				//it may be the case that old parent, pushed to the botton, is still smaller thant its new children
				heapify(extremeIdx, comparator);
			}
		}
		
		//Move it to HEAP class
		/**
		 * Returns parent index, given index of node in question
		 */
		public int parentIdx(int nodeIdx) {
			return ((nodeIdx - 1) / 2);
		}
		
		/**
		 * Returns index of the left child of node in question
		 */
		public int leftChildIdx(int nodeIdx) {
			return (2 * nodeIdx + 1);
		}

		/**
		 * Returns index of the right child of node in question
		 */
		public int rightChildIdx(int nodeIdx) {
			return (2 * nodeIdx + 2);
		}
	}


	/** The storage manager for this operator. */
	private StorageManager sm;

	/** The name of the temporary file for the output. */
	private String outputFile;

	/** The manager that undertakes output relation I/O. */
	private RelationIOManager outputMan;

	/** The slots that act as the sort keys. */
	private int [] slots;

	/** Number of buffers (i.e., buffer pool pages and 
	 * output files). */
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
	 * @param buffers the number of buffers (i.e., run files) to be
	 * used for the sort.
	 * @throws EngineException thrown whenever the sort operator
	 * cannot be properly initialized.
	 */
	public ExternalSort(Operator operator, StorageManager sm,
			int [] slots, int buffers) 
					throws EngineException {

		super(operator);
		this.sm = sm;
		this.slots = slots;
		this.buffers = buffers;
		try {
			// create the temporary output files
			initTempFiles();
		}
		catch (StorageManagerException sme) {
			throw new EngineException("Could not instantiate external sort",
					sme);
		}
	} // ExternalSort()


	/**
	 * Initialises the temporary files, according to the number
	 * of buffers.
	 * 
	 * @throws StorageManagerException thrown whenever the temporary
	 * files cannot be initialised.
	 */
	protected void initTempFiles() throws StorageManagerException {
		////////////////////////////////////////////
		//
		// initialise the temporary files here
		// make sure you throw the right exception
		// in the event of an error
		//
		// for the time being, the only file we
		// know of is the output file
		//
		////////////////////////////////////////////
		outputFile = FileUtil.createTempFileName();
	} // initTempFiles()


	/**
	 * Sets up this external sort operator.
	 * 
	 * @throws EngineException thrown whenever there is something wrong with
	 * setting this operator up
	 */
	public void setup() throws EngineException {
		returnList = new ArrayList<Tuple>();
		try {
			////////////////////////////////////////////
			//
			// this is a blocking operator -- store the input
			// in a temporary file and sort the file
			//
			////////////////////////////////////////////

			////////////////////////////////////////////
			//
			// YOUR CODE GOES HERE
			//            // store the left input

			//with this, the input is already buffered
			Relation rel = getInputOperator().getOutputRelation();
			RelationIOManager man =
					new RelationIOManager(getStorageManager(), rel, fileName);
			boolean done = false;
			while (! done) {
				Tuple tuple = getInputOperator().getNext();
				if (tuple != null) {
					done = (tuple instanceof EndOfStreamTuple);
					if (!done) man.insertTuple(tuple);
				}
			}
			//we have opened file on disk, completely paginated

			//parameters of external sort
			/**
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

			
			/** ANOTHER CLASSES
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
			////////////////////////////////////////////



			////////////////////////////////////////////
			//
			// the output should reside in the output file
			//
			////////////////////////////////////////////

			outputMan = new RelationIOManager(sm, getOutputRelation(),
					outputFile);
			outputTuples = outputMan.tuples().iterator();
		}
		catch (Exception sme) {
			throw new EngineException("Could not store and sort"
					+ "intermediate files.", sme);
		}
	} // setup()


	/**
	 * Cleanup after the sort.
	 * 
	 * @throws EngineException whenever the operator cannot clean up
	 * after itself.
	 */
	public void cleanup () throws EngineException {
		try {
			////////////////////////////////////////////
			//
			// make sure you delete the intermediate
			// files after sorting is done
			//
			////////////////////////////////////////////

			////////////////////////////////////////////
			//
			// right now, only the output file is 
			// deleted
			//
			////////////////////////////////////////////
			sm.deleteFile(outputFile);
		}
		catch (StorageManagerException sme) {
			throw new EngineException("Could not clean up final output.", sme);
		}
	} // cleanup()


	/**
	 * The inner method to retrieve tuples.
	 * 
	 * @return the newly retrieved tuples.
	 * @throws EngineException thrown whenever the next iteration is not 
	 * possible.
	 */    
	protected List<Tuple> innerGetNext () throws EngineException {
		try {
			returnList.clear();
			if (outputTuples.hasNext()) returnList.add(outputTuples.next());
			else returnList.add(new EndOfStreamTuple());
			return returnList;
		}
		catch (Exception sme) {
			throw new EngineException("Could not read tuples " +
					"from intermediate file.", sme);
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
	 * Operator class abstract interface -- sets the ouput relation of
	 * this sort operator.
	 * 
	 * @return this operator's output relation.
	 * @throws EngineException whenever the output relation of this
	 * operator cannot be set.
	 */
	protected Relation setOutputRelation() throws EngineException {
		return new Relation(getInputOperator().getOutputRelation());
	} // setOutputRelation()

} // ExternalSort
