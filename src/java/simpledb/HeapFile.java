package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        try {
            RandomAccessFile fileReader = new RandomAccessFile(f, "r");
            byte[] buf = new byte[BufferPool.getPageSize()];
            
            fileReader.seek(BufferPool.getPageSize() * pid.getPageNumber());
            fileReader.read(buf);
            fileReader.close();
            
            return new HeapPage((HeapPageId) pid, buf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
    
    // inner class to support HeapFile.iterator()
    // inherits by DbFileIterator
    public class HeapFileIterator implements DbFileIterator {
        // fields
        private HeapFile hf;
        private TransactionId tid;
        private int pageNum;
        private Iterator<Tuple> tuples;
        
        // constructor
        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
        }
        
        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            pageNum = 0;
            tuples = openHelper(pageNum).iterator();
        }
        
        public HeapPage openHelper(int pageNumber) throws DbException, TransactionAbortedException {
            return (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(hf.getId(), pageNumber), Permissions.READ_ONLY);
        }
        
        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // there is no next item
            if (tuples == null || pageNum > hf.numPages()) {
                return false;
            }
            
            // looking for tuples available from page to page
            while (!tuples.hasNext()) {
                pageNum += 1;
                
                if (pageNum >= hf.numPages()) { return false; }
                
                tuples = openHelper(pageNum).iterator();
            }
            // loop terminated, found tuples
            return true;
        }
        
        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws NoSuchElementException, DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            
            return tuples.next();
        }
        
        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }
        
        /**
         * Closes the iterator.
         */
        public void close() {
            pageNum = 0;
            tuples = null;
        }
    }
}

