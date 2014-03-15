/*
 * Created on Dec 7, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.IOException;

import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.dejave.attica.engine.predicates.TupleSlotPointer;
import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.FileUtil;


/**
 * RelationIOManager: The basic class that undertakes relation I/O.
 *
 * @author sviglas
 */
public class RelationIOManager {
	
    /** The relation of this manager. */
    private Relation relation;
	
    /** This manager's storage manager. */
    private StorageManager sm;
	
    /** The filename for the relation. */
    private String filename;
	
    /**
     * Constructs a new relation I/O manager.
     * 
     * @param sm this manager's storage manager.
     * @param relation the relation this manager handles I/O for.
     * @param filename the name of the file this relation is stored
     * in.
     */
    public RelationIOManager(StorageManager sm,
                             Relation relation, 
                             String filename) {
        this.sm = sm;
        this.relation = relation;
        this.filename = filename;
    } // RelationIOManager()
	
    /**
     * Inserts a new tuple into this relation.
     * 
     * @param tuple the tuple to be inserted.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple(Tuple tuple) throws StorageManagerException {
        insertTuple(tuple, true);
    } // insertTuple()

    
    /**
     * Inserts a new tuple specified as a list of comparable values
     * into this relation.
     *
     * @param values the list of comparables to be inserted.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple (List<Comparable> values)
        throws StorageManagerException {
        
        insertTuple(new Tuple(new TupleIdentifier(null, 0), values), true);
    } // insertTuple()

    
    /**
     * Inserts a new tuple into this relation.
     * 
     * @param tuple the tuple to be inserted.
     * @param newID re-assigns the tuple id if set to <pre>true</pre>.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple(Tuple tuple, boolean newID) 
	throws StorageManagerException {

        // to be honest, going over the code I have no idea why we may
        // not be re-assigning the tuple id, but I guess I was
        // thinking of something back then.
        try {
            // read in the last page of the file
            int pageNum = FileUtil.getNumberOfPages(getFileName());
            pageNum = (pageNum == 0) ? 0 : pageNum-1;
            PageIdentifier pid = 
                new PageIdentifier(getFileName(), pageNum);
            Page page = sm.readPage(relation, pid);
            int num = 0;
            if (page.getNumberOfTuples() != 0) {
                Tuple t = page.retrieveTuple(page.getNumberOfTuples()-1);
                num = t.getTupleIdentifier().getNumber()+1;
            }

            if (newID)
                tuple.setTupleIdentifier(new TupleIdentifier(getFileName(),
                                                             num));
            
            //long tn = page.getNumberOfTuples();
            if (! page.hasRoom(tuple)) {
                page = new Page(relation, new PageIdentifier(getFileName(),
                                                             pageNum+1));
                FileUtil.setNumberOfPages(getFileName(), pageNum+2);
            }
            page.addTuple(tuple);
            sm.writePage(page);
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            throw new StorageManagerException("I/O Error while inserting tuple "
                                              + "to file: " + getFileName()
                                              + " (" + e.getMessage() + ")", e);
        }
    } // insertTuple ()


    /**
     * Wrapper for castAttributes() with a list of comparables as the
     * parameter. See the notes there. (Package visible, because this
     * is embarrassing.)
     *
     * @param crap the crap needed by the other cast attributes
     * method.
     * @throws StorageManagerException thrown whenever the cast is not
     * possible.
     */
    void castAttributes(Tuple crap) throws StorageManagerException {
        castAttributes(crap.getValues());
    }

    /**
     * Package-visible (due to it being embarrassing) method to cast
     * comparables to correct comparable type. Confused? Yeah, you
     * should be.
     *
     * Assumes all comparables are strings and casts them to the
     * correct comparable type. I can't even begin to describe how
     * stupid this is. Actually, I can begin to describe how stupid it
     * is, but I won't finish on time. So, there.
     *
     * No, really. This is stupid. We're talking about a language that
     * after ensuring type-safety at compile-time, it cannot guarantee
     * it at run-time after serialization. This is just insane.
     *
     * @param crap the list of crap you feed the tuple.
     * @throws StorageManagerException when crap smells too bad.
     */
    void castAttributes(List<Comparable> crap) throws StorageManagerException {
        
        for (int i = 0; i < crap.size(); i++) {
            Comparable c = crap.get(i);

            Class<? extends Comparable> type =
                relation.getAttribute(i).getType();            
            if (type.equals(Byte.class))
                crap.set(i, new Byte((String) c));
            else if (type.equals(Short.class))
                crap.set(i, new Short((String) c));
            else if (type.equals(Integer.class))
                crap.set(i, new Integer((String) c));
            else if (type.equals(Long.class))
                crap.set(i, new Long((String) c));
            else if (type.equals(Float.class))
                crap.set(i, new Float((String) c));
            else if (type.equals(Double.class))
                crap.set(i, new Double((String) c));
            else if (type.equals(Character.class))
                crap.set(i, new Character(((String) c).charAt(0)));
            else if (! type.equals(String.class))
                throw new StorageManagerException("Unsupported type: "
                                                  + type + ".");
        }
    }
	
    /**
     * The name of the file for this manager.
     * 
     * @return the name of the file of this manager
     */
    public String getFileName () {
        return filename;
    } // getFileName()


    /**
     * Opens a page iterator over this relation.
     *
     * @return a page iterator over this relation.
     * @throws IOException whenever the iterator cannot be
     * instantiated from disk.
     * @throws StorageManagerException whenever the iterator cannot be
     * created after the file has been loaded.
     */
    public Iterable<Page> pages()
        throws IOException, StorageManagerException {
        
        return new PageIteratorWrapper();
    } // pages()

    /**
     * Opens a tuple iterator over this relation.
     *
     * @return a tuple iterator over this relation.
     * @throws IOException whenever the iterator cannot be
     * instantiated from disk.
     * @throws StorageManagerException whenever the iterator cannot be
     * created after the file has been loaded.
     */
    public Iterable<Tuple> tuples()
        throws IOException, StorageManagerException {
        
        return new TupleIteratorWrapper();
    } // tuples()

    
    /**
     * The basic iterator over pages of this relation.
     */
    class PageIteratorWrapper implements Iterable<Page> {
        /** The current page of the iterator. */
        private Page currentPage;

        /** The number of pages in the relation. */
        private int numPages;

        /**
         * Constructs a new page iterator.
         */
        public PageIteratorWrapper()
            throws IOException, StorageManagerException {

            numPages = FileUtil.getNumberOfPages(getFileName());
        } // PageIteratorWrapper()

        /**
         * Returns an iterator over pages.
         * @return the iterator over pages.
         */
        public ListIterator<Page> iterator() {
            return new PageListIterator(0); // new Iterator
        } // iterator()
        
        public ListIterator<Page> listIterator(int nextIdx) {
            return new PageListIterator(nextIdx); // new Iterator
        } // iterator()
        
        class PageListIterator implements ListIterator<Page> {
            /** The current page offset. */
            private int pageOffset;
            
        	public PageListIterator(int nextIdx) {
                pageOffset = nextIdx;
                assert(0 <= pageOffset && pageOffset < numPages);
        	}
        	
            public boolean hasNext() {
                return pageOffset < numPages;
            } // hasNext()
            
            public Page next() throws NoSuchElementException {
                try {
                    currentPage =
                        sm.readPage(relation,
                                    new PageIdentifier(getFileName(),
                                                       pageOffset++));
                    return currentPage;
                }
                catch (StorageManagerException sme) {
                    throw new NoSuchElementException("Could not read "
                                                     + "page to advance "
                                                     + "the iterator.");
                                                     
                }
            } // next()
            public void remove() throws UnsupportedOperationException {
                throw new UnsupportedOperationException("Cannot remove "
                                                        + "from page "
                                                        + "iterator.");
            } // remove()
            
			@Override
			public void add(Page arg0) {
				throw new UnsupportedOperationException("Cannot add page with iterator.");
				
			}//add()
			
			@Override
			public boolean hasPrevious() {
				return pageOffset > 0;
			}//hasPrevious()
			
			@Override
			public int nextIndex() {
				return pageOffset;
			}//nextIndex()
			
			@Override
			public Page previous() {
                try {
                    currentPage =
                        sm.readPage(relation,
                                    new PageIdentifier(getFileName(),
                                                       --pageOffset));
                    return currentPage;
                }
                catch (StorageManagerException sme) {
                    throw new NoSuchElementException("Could not read "
                                                     + "page to retreat "
                                                     + "the iterator.");
                                                     
                }
			}//previous()
			
			@Override
			public int previousIndex() {
				return pageOffset - 1;
			}//previousIndex()
			
			@Override
			public void set(Page arg0) {
				throw new UnsupportedOperationException("Cannot set the page at iterator position.");
			}//set()
			
        };
    } // PageIteratorWrapper


    /**
     * The basic iterator over tuples of this relation.
     */
    class TupleIteratorWrapper implements Iterable<Tuple> {  
    	PageIteratorWrapper pageItWrapper = null;
    	
        /**
         * Constructs a new tuple iterator.
         */
        public TupleIteratorWrapper()
            throws IOException, StorageManagerException {
        	pageItWrapper = (PageIteratorWrapper)pages();
        } // TupleIterator()

        public ListIterator<Tuple> iterator() {
            return new TupleListIterator();
        } // iterator()
        
        public ListIterator<Tuple> listIterator(int nextIdx) {
        	return new TupleListIterator(nextIdx, relation);
        }
        
        public class TupleListIterator implements ListIterator<Tuple> {
            /** The page iterator. */
            private ListIterator<Page> pagesIt;
            
            /** The single-page tuple iterator. */
            private ListIterator<Tuple> tuplesIt;
            
            /**
             * keeps track of the direction which page iterator moved to last time.
             * This needs to be included, since Java iterators are not like C++ ones.
             * The problem is as follows:
             * 	- when next() is invoked and we are done with current page, we get next one with PageListIterator::next() to get another page and iterate with tuples over it;
             *  - however if someone now starts iterating back, previous() will call PageListIterator::previous() which will not give the previous page, but the actual one!
             *  -> Java iterators it.next() == it.previous();
             * This phenomenon has to be taken care of when calling hasPrevious() next() and previous().
             * Fortunately, it's not that complicated for hasNext() since we may be on the last page only if iterated forwards (or invoked listIterator() with last page tuples index -
             * - but this still acts like iterated forwards).
             */
            boolean lastPageDirIsForward = true;
            
            int currentIdx = 0;
                        
            public TupleListIterator() {
            	pagesIt = pageItWrapper.listIterator(0);
            	tuplesIt = pagesIt.next().listIterator();
            	lastPageDirIsForward = true;
            }
            
            public TupleListIterator(int nextIdx, Relation tupleRelation) {            	
            	//NOTE: if the size of tuples was constant, we could use following code
            	/* pagesIt = pageItWrapper.listIterator(0);
            	if (pagesIt.hasNext()) {
            		Page p = pageItWrapper.listIterator(0).next();
            		int tupleSize = 0;
            		if (p.getNumberOfTuples() > 0) {
            			Tuple t = p.iterator().next();
            			tupleSize = TupleIOManager.byteSize(tupleRelation, t);
            			final int tuplesPerPage = Sizes.PAGE_SIZE / tupleSize;
            			pagesIt = pageItWrapper.listIterator(nextIdx / tuplesPerPage);
            			p = pagesIt.next();
            			tuplesIt = p.listIterator(nextIdx % tuplesPerPage);
            			currentIdx = nextIdx;
                		lastPageDirIsForward = true;
            		}
            		else {
            			throw new IndexOutOfBoundsException();
            		}
            	} */

            	//NOTE: however it's not constant, so we use the following code (bad, since to reach some tuple we need to scan from 0 page)
            	//size of the tuple is not guaranteed to be constant, so we need to iterate over all pages
                pagesIt = pageItWrapper.listIterator(0);
                Page p = pagesIt.next();
                int tuplesNoInPreviousPages = p.getNumberOfTuples();
                while (tuplesNoInPreviousPages <= nextIdx && pagesIt.hasNext()) {
                	p = pagesIt.next();
                	tuplesNoInPreviousPages += p.getNumberOfTuples();
                }
                
                //we are currently on a page with the proper tuple, or have run out of pages
                if (tuplesNoInPreviousPages >= nextIdx) {
                	tuplesIt = p.listIterator(nextIdx - tuplesNoInPreviousPages + p.getNumberOfTuples());
                	currentIdx = nextIdx;
                	lastPageDirIsForward = true;
                }
                else {
                	throw new IndexOutOfBoundsException();
                }
            }
            
        	/**
        	 * Checks whether there are more tuples in this iterator.
        	 *
        	 * @return <code>true</code> if there are more tuples,
        	 * <code>false</code> otherwise.
        	 */
            public boolean hasNext() {
                return (tuplesIt.hasNext() ||	
                		pagesIt.hasNext());
            } // hasNext()
            
            public Tuple next() throws NoSuchElementException {
            	Tuple tuple = null;
            	try {
            		tuple = tuplesIt.next();
            	}
            	catch (NoSuchElementException e) {

            		lastPageDirIsForward = true;

            		tuplesIt = (ListIterator<Tuple>)pagesIt.next().iterator();
            		tuple = tuplesIt.next();
            	}
            	currentIdx++;
            	                    
                return tuple;
            } // next()
            
            public void remove() throws UnsupportedOperationException {
                throw new UnsupportedOperationException("Cannot remove "
                                                        + "from tuple "
                                                        + "iterator.");
            } // remove()
			@Override
			public void add(Tuple arg0) {
                throw new UnsupportedOperationException("Cannot add to tuple iterator.");
			}
			@Override
			public boolean hasPrevious() {
				//we don't look at pagesIt.hasPrevious(), since tuplesIt is an iterator 
				//already in a previous page
				return (tuplesIt.hasPrevious() ||
						(lastPageDirIsForward ? pagesIt.previousIndex() > 0 : pagesIt.hasPrevious()));
			}
			
			@Override
			public int nextIndex() {
				return currentIdx;
			}
			
			@Override
			public Tuple previous() {					
				Tuple tuple = null;
				try {
					tuple = tuplesIt.previous();
				}
				catch (NoSuchElementException e) {
					Page page = null;
					if (lastPageDirIsForward) {
						pagesIt.previous();
						lastPageDirIsForward = false;
					}
					page = pagesIt.previous();
					tuplesIt = page.listIterator(page.getNumberOfTuples());
					tuple = tuplesIt.previous();
				}
                --currentIdx;
                
                return tuple;
			}
			
			@Override
			public int previousIndex() {
				return currentIdx - 1;
			}
			
			@Override
			public void set(Tuple arg0) {
				throw new UnsupportedOperationException("Cannot set the tuple with tuple iterator.");
			}
        };//TupleListIterator
    } // TupleIteratorWrapper

    
    /**
     * Debug main.
     */
    public static void main (String [] args) {
        try {
            BufferManager bm = new BufferManager(100);
            StorageManager sm = new StorageManager(null, bm);
            
            List<Attribute> attributes = new ArrayList<Attribute>();
            attributes.add(new Attribute("integer", Integer.class));
            attributes.add(new Attribute("string", String.class));
            Relation relation = new Relation(attributes);
            String filename = args[0];
            sm.createFile(filename);
            RelationIOManager manager = 
                new RelationIOManager(sm, relation, filename);
            
            final int tupleSize = getTestTupleSize(relation, filename);
            final int tuplesNoPerPage = Sizes.PAGE_SIZE / tupleSize;
            
            int tuplesToWrite = tuplesNoPerPage + 1;
            
            //crossing the boundary of 2 pages Pages
    		writeRelation(filename, manager, tuplesToWrite);
            testIWholeRelationIteration(filename, manager, tuplesToWrite);
            testPartialRelationIteration(manager, tuplesToWrite - 2, 2);
            sm.deleteFile(filename);
            
            //test over only one page
            sm.createFile(filename);
            tuplesToWrite = tuplesNoPerPage;
            writeRelation(filename, manager, tuplesToWrite);
            testIWholeRelationIteration(filename, manager, tuplesToWrite);
            testPartialRelationIteration(manager, 0, tuplesToWrite);
            sm.deleteFile(filename);
            
            //4 pages, cross middle two
            sm.createFile(filename);
            tuplesToWrite = 4 * tuplesNoPerPage;
            writeRelation(filename, manager, tuplesToWrite);
            testIWholeRelationIteration(filename, manager, tuplesToWrite);
            testPartialRelationIteration(manager, 0, tuplesToWrite);
            sm.deleteFile(filename);

        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }	
    } // main()

	private static void testPartialRelationIteration(RelationIOManager manager,
			int startIdx, int tuplesToRead) throws IOException,
			StorageManagerException, UnexpectedException {
		//iteration from a give point few tuples forth and back
		System.out.println("Forward and backward list iteration...");
		TupleIteratorWrapper tiw = (TupleIteratorWrapper)manager.tuples();
		ListIterator<Tuple> tupleIt = tiw.listIterator(startIdx);
		HashMap<Tuple, Integer> checkTuples = new HashMap<Tuple, Integer>();
		Tuple tuple1 = null;
		for (int i = 0; i < tuplesToRead; ++i) {
			tuple1 = tupleIt.next();
			checkTuples.put(tuple1, 1);
			System.out.print("+");
		}
		
		System.out.println();
		Integer rmdTupleId = null;
		while (tupleIt.previousIndex() >= startIdx) {
			tuple1 = tupleIt.previous();
			if (! checkTuples.containsKey(tuple1)) {
				throw new UnexpectedException("Tuple not in a hash while traversing back!");
			}
			rmdTupleId = checkTuples.remove(tuple1);
			System.out.print("-");
		}
		//make sure all were deleted
		if (0 != checkTuples.size()) {
			throw new UnexpectedException("Some tuples are not removed!");
		}
		else {
			System.out.print("Forward and backward iteration works!");
		}
	}

	private static void testIWholeRelationIteration(String filename,
			RelationIOManager manager, int tuplesTotalNo)
			throws StorageManagerException, IOException, UnexpectedException {
		System.out.println("Opening tuple cursor...");

		//forward iteration
		System.out.println("Forward iteration...");
		int tuplesFwdRead = 0;
		for (Tuple tuple : manager.tuples()) {
		    System.out.print(".");
		    ++tuplesFwdRead;
		}
		
		if (tuplesFwdRead != tuplesTotalNo) {
			throw new UnexpectedException("Different number of tuples written and read");
		}
		else {
			System.out.println("Forward iteration (" + tuplesFwdRead + ") OK!");
		}
		
		//backward iteration check
		System.out.println("Backward iteration...");
		TupleIteratorWrapper tiw = (TupleIteratorWrapper)manager.tuples();
		ListIterator<Tuple> bckTupleIt = tiw.listIterator(tuplesTotalNo);
		int tuplesBckRead = 0;
		Tuple tuple1 = null;
		while (bckTupleIt.hasPrevious()) {
			tuple1 = bckTupleIt.previous();
			++tuplesBckRead;
			System.out.print("+");
		}
		if (tuplesBckRead != tuplesTotalNo) {
			throw new UnexpectedException("Different number of tuples written and read in  backward direction!");
		}
		else {
			System.out.println("Backward iteration (" + tuplesFwdRead + ") OK!");
		}
	}

	private static int writeRelation(String filename,
			RelationIOManager manager, int tuplesToWrite)
			throws StorageManagerException {
		int tuplesWritten = 0;
		System.out.println("Inserting...");
		for (int i = 0; i < tuplesToWrite; i++) {
		    List<Comparable> v = new ArrayList<Comparable>();
		    v.add(new Integer(i));
		    v.add(new String("bla"));
		    Tuple tuple = new Tuple(new TupleIdentifier(filename, i), v);
		    System.out.print(".");
		    manager.insertTuple(tuple);
		    ++tuplesWritten;
		}
		
		System.out.println("Tuples successfully inserted.");
		return tuplesWritten;
	}

	private static int getTestTupleSize(Relation relation, String filename) {
		List<Comparable> v = new ArrayList<Comparable>();
		v.add(new Integer(0));
		v.add(new String("bla"));
		Tuple tuple = new Tuple(new TupleIdentifier(filename, 0), v);
		return TupleIOManager.byteSize(relation, tuple);
	}
    
} // RelationIOManager
