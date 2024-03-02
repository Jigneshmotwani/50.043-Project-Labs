package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

/**
 * Helper class that implements the DbFileIterator for HeapFiles
 */
public class HeapFileIterator extends AbstractDbFileIterator {
    private int pageIdx;
    private final TransactionId tid;
    private final int tableId;
    private final int pageNum;
    private Iterator<Tuple> iter;

    public HeapFileIterator(int tableId, TransactionId tid, int pageNum) {
        this.tid = tid;
        this.tableId = tableId;
        this.pageNum = pageNum;
        pageIdx = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (iter == null)
            return null;

        if (iter.hasNext())
            return iter.next();

        while (pageIdx < pageNum - 1) {
            pageIdx++;
            open();
            if (iter == null)
                return null;
            if (iter.hasNext())
                return iter.next();
        }

        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(tableId, pageIdx);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        iter = page.iterator();
    }

    @Override
    public void close() {
        super.close();
        iter = null;
        pageIdx = 0;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}