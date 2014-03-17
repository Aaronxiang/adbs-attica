/*
 * Created on Feb 11, 2004 by sviglas
 *
 * Modified on Feb 17, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.FileUtil;
import org.dejave.attica.storage.IntermediateTupleIdentifier;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.Sizes;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIOManager;
import org.dejave.util.ExtensiblePagedList;
import org.dejave.util.OperatorTuplesIterator;
import org.dejave.util.SortMerger2;
import org.dejave.util.SortMerger2.MergerBuffer;

/**
 * MergeJoin: Implements a merge join. The assumptions are that the
 * input is already sorted on the join attributes and the join being
 * evaluated is an equi-join.
 *
 * @author sviglas
 *
 * Krzysztow: 
 * The class implemenets a merge join algorithm, which execution takes place in few classes: OperatorSortMerger (inheriting from SortMerger2) with ExtensiblePagedList (wrapped in PagedListMergerBuffer) used as buffer.
 * How algorithm works is explained in {@link SortMerger2::doMerge()}. There were few options I chose from:
 * - without any additional buffer, but then Operator::tuples()::iterators() would have to be both-direction (next() & previous()). These seems like not the correct way, since would require architectural changes
 * 	and I have a feeling wouldn't work well with some of the operators. If it was possible, one could use {@link SortMerger} class.
 *  (if we had access to the intermediate files (after the sort phase), we could modify tuples() and pages() iterators (as in RelationalIOManager2.java and Page2.java (the test in Main() in RelationalIOManager2.java works))
 * 	and then work one these files directly - however I didn't do that, since it breaks cohesion and is a hack).
 * - with additional buffer:
 *  The buffer is needed, since once we read tuple from Operator it cannot be re-read (which is necessary for joining with potential next tuple from second input). This had two options:
 * 	- buffering the group of equivalent tuples in a first group in list - not good, since we have no control over the memory consumption;
 * 	- buffering those tuples in a attica Page, which whenever filled is flushed to disk. This ensures we use no more than one in-memory page and in fact hurts performance only if:	
 * 		* the number of tuples in a group is larger than the one that fits into one page;
 * 		* we are so unfortunate that StorageManager evicted already a page - not so likely, since we are localized (in time) with page usage (and SM has LRU algorithm).
 * 
 * Note: I don't use {@link Predicate} and {@link PredicateEvaluator} classes, since their fucntionality is contained within SingleSlotTupleComparator. It's not a limitation, since:
 * - comparator is more general - not only returns true/false but also the type of relation (smaller/larger);
 * - MergeJoin is used for only equi-joins - even if it wasn't, then we would have problems with sorting and comparing tuples according to predicate (so if 
 * 		ever it's going to change to other joins, not only this would have to be modified);
 * 
 * Note2: I removed inheritance from NestedLoopsJoins, since I see no reason to do it (there was also problem with outputFile being shadowed with MergeJoin.outputFile member) + uncommented MergeJoin::cleanup() method.
 */

public class MergeJoin extends PhysicalJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new mergejoin operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
	public MergeJoin(Operator left, 
                     Operator right,
                     StorageManager sm,
                     int leftSlot,
                     int rightSlot,
                     Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        returnList = new ArrayList<Tuple>(); 
        try {
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // MergeJoin()


    /**
     * Initialise the temporary files -- if necessary.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        outputFile = FileUtil.createTempFileName();
    } // initTempFiles()

    class SingleSlotTupleComparator implements Comparator<Tuple> {
    	int leftSlot = -1;
    	int rightSlot = -1;
    	
    	public SingleSlotTupleComparator(int lefSlot, int rightSlot) {
    		this.leftSlot = lefSlot;
    		this.rightSlot = rightSlot;
    	}
    	
		@Override
		public int compare(Tuple leftVal, Tuple rightVal) {
			return leftVal.getValue(leftSlot).compareTo(rightVal.getValue(rightSlot));
		}
    }
    
    /**
     * Sets up this merge join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    
    @Override
    protected void setup() throws EngineException {
    	PagedListMergerBuffer buffer = null;
    	try {
    		//section of what is given
        	Operator leftOperator = getInputOperator(LEFT);
        	Operator rightOperator = getInputOperator(RIGHT);
        	Relation relation = leftOperator.getOutputRelation();
        	StorageManager sm = getStorageManager();        	

        	//create a output file and its manager for a merger
    		sm.createFile(outputFile);
            outputMan = new RelationIOManager(getStorageManager(), 
                                              getOutputRelation(),
                                              outputFile);
        	
            //tuples iterators wrappers that are more easy for me (probably overhead doesn't matter)
        	OperatorTuplesIterator leftIt = new OperatorTuplesIterator(leftOperator);
        	OperatorTuplesIterator rightIt = new OperatorTuplesIterator(rightOperator);
        	
        	//create comparator
        	Comparator<Tuple> comparator = new SingleSlotTupleComparator(leftSlot, rightSlot);
        	//buffer for keeping groups of equivalent tuples
        	buffer = new PagedListMergerBuffer(
        			TupleIOManager.byteSize(relation, leftIt.peek()), Sizes.PAGE_SIZE, relation, sm);
        	
        	OperatorSortMerger merger = new OperatorSortMerger(comparator, buffer, outputMan);
        	merger.doMerge(leftIt, rightIt);
        	
        	//don't throw exception if cannot remove temporary file, since we did our job and results may be used
        	try {buffer.cleanup();} catch (EngineException e) {
        		System.err.println("Cannot clean intermediate file after merge is done!");
        	}
        	
            outputTuples = outputMan.tuples().iterator();
        }
        catch (Exception e) {
        	if (null != buffer) {
        		try {buffer.cleanup();} catch (Exception e1) {};
        	}
        	
        	
        	throw new EngineException("Can't do sort merge.", e);
        }
    } // setup()
    
    
    /**
     * Cleans up after the join.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    @Override
    protected void cleanup() throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete any temporary files
            //
            ////////////////////////////////////////////
            
            getStorageManager().deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not clean up " +
                                                     "final output");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // cleanup()
    

    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Inner tuple processing.  Returns an empty list but if all goes
     * well it should never be called.  It's only there for safety in
     * case things really go badly wrong and I've messed things up in
     * the rewrite.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()

    
    /**
     * Textual representation
     */
    protected String toStringSingle () {
        return "mj <" + getPredicate() + ">";
    } // toStringSingle()
    
    /**
     * Implementation of the list-like MergeBuffer which is backed up with 
     * ExtensiblePagedList buffer.
     * For descriptions of what is expected from methods, see MergerBuffer.
     * @author krzysztow
     *
     */
    private static class PagedListMergerBuffer implements MergerBuffer<Tuple> {
		ExtensiblePagedList listBuffer = null;
		
		public PagedListMergerBuffer(int tupleSize, int pageSize, Relation rel, StorageManager sm) 
				throws EngineException, StorageManagerException {
        	String arrayListFile = FileUtil.createTempFileName();
        	sm.createFile(arrayListFile);
			listBuffer = new ExtensiblePagedList(tupleSize, pageSize, rel, sm, arrayListFile);
		}

		/**
		 * Resets the underlying buffer structure.
		 */
		@Override
		public void reset() 
				throws Exception {
			listBuffer.reset();
		}

		/**
		 * Adds value to the end of the buffer.
		 */
		@Override
		public void addValue(Tuple value) 
				throws Exception {
			listBuffer.addTuple(value);
		}

		/**
		 * Iterator over entire buffer.
		 */
		@Override
		public Iterator<Tuple> iterator() 
				throws Exception {
			return listBuffer.iterator();
		}

		/*
		 * Cleans up allocate resources for a buffer
		 */
		public void cleanup() 
				throws EngineException {
			if (null != listBuffer)
				listBuffer.cleanup();
		}
	}
    
    /**
     * Concrete SortMerger2 that merges two sorted Tuples inputs into the output relation.
     * @author krzysztow
     *
     */
    public class OperatorSortMerger extends SortMerger2<Tuple> {
    	private RelationIOManager manager = null;
    	
    	public OperatorSortMerger(Comparator<Tuple> comparator, SortMerger2.MergerBuffer<Tuple> buffer, RelationIOManager manager) {
    		super(comparator, buffer);
    		this.manager = manager;
    	}

		@Override
    	public void mergeValues(Tuple first, Tuple second) 
    			throws Exception {
    		Tuple combinedTuple = combineTuples(first, second);
    		try {
				manager.insertTuple(combinedTuple);
			} catch (StorageManagerException e) {
				throw new Exception("Can't insert merged tuples.", e);
			}
    	}
		
	    /**
	     * Copied from NestedLoopsJoin
	     * Given two tuples, combine them into a single one.
	     * 
	     * @param left the left tuple.
	     * @param right the right tuple.
	     * @return a new tuple with the left and right tuples combined.
	     */    
	    protected Tuple combineTuples(Tuple left, Tuple right) {
	        List<Comparable> v = new ArrayList<Comparable>();
	        v.addAll(left.getValues());
	        v.addAll(right.getValues());
	        return new Tuple(new IntermediateTupleIdentifier(tupleCounter++), v);
	    } // combineTuples()

    }

} // MergeJoin
