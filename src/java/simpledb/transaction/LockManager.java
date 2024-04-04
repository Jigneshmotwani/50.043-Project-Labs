package simpledb.transaction;

import java.util.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.ReadWriteLock;

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
public class LockManager {
    HashMap<PageId, ReadWriteLock> pageLock;
    HashMap<TransactionId, Set<TransactionId>> dependencyGraph;
    HashMap<TransactionId, Set<PageId>> pagesTid;

    public LockManager() {
        this.pageLock = new HashMap<PageId, ReadWriteLock>();
        this.dependencyGraph = new HashMap<TransactionId, Set<TransactionId>>();
        this.pagesTid = new HashMap<TransactionId, Set<PageId>>();
    }

    private ReadWriteLock getLock(PageId pid) {
        if (!this.pageLock.containsKey(pid)) {
            this.pageLock.put(pid, new ReadWriteLock());
            // create if doesn't exist
        }
        return this.pageLock.get(pid);
    }

    private Set<PageId> getPages(TransactionId tid) {
        if (!this.pagesTid.containsKey(tid)) {
            this.pagesTid.put(tid, new HashSet<PageId>());
            // create if doesn't exist
        }
        return this.pagesTid.get(tid);
    }

    public Set<PageId> getPagesHeldByLock(TransactionId tid) {
        if (this.pagesTid.containsKey(tid)) {
            return this.pagesTid.get(tid);
        }
        return null;
    }

    public void acquireReadLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        ReadWriteLock lock;
        synchronized (this) {
            lock = this.getLock(pid);
            if (lock.lockHeldBy(tid)) {
                return;
            } 

            if (!lock.getHolders().isEmpty() && lock.isLockedExclusively()) {
                this.dependencyGraph.put(tid, lock.getHolders());
                if (this.deadlock(tid)) {
                    this.dependencyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }

        lock.readLock(tid);
        synchronized (this) {
            this.dependencyGraph.remove(tid);
            this.getPages(tid).add(pid);
        }
    }

    public void acquireWriteLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        ReadWriteLock lock;
        synchronized (this) {
            lock = this.getLock(pid);
            if (lock.isLockedExclusively() && lock.lockHeldBy(tid)) {
                return;
            }
            if (!lock.getHolders().isEmpty()){
                this.dependencyGraph.put(tid, lock.getHolders());
                if (this.deadlock(tid)) {
                    this.dependencyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }

        lock.writeLock(tid);
        synchronized (this) {
            this.dependencyGraph.remove(tid);
            this.getPages(tid).add(pid);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!this.pageLock.containsKey(pid)) {
            return;
        }

        ReadWriteLock lock = this.pageLock.get(pid);
        lock.unlock(tid);
        this.pagesTid.get(tid).remove(pid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        if (!this.pagesTid.containsKey(tid)) {
            return;
        }

        Set<PageId> pages = this.pagesTid.get(tid);
        for (Object pageId: pages.toArray()) {
            this.releaseLock(tid, ((PageId) pageId));
        }
        this.pagesTid.remove(tid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return this.pagesTid.containsKey(tid) && this.pagesTid.get(tid).contains(pid);
    }

    private boolean deadlock(TransactionId tid) {
        Set<TransactionId> vis = new HashSet<TransactionId>();
        Queue<TransactionId> queue = new LinkedList<TransactionId>();
        vis.add(tid);
        queue.offer(tid);
        while (!queue.isEmpty()) {
            TransactionId head = queue.poll();
            if (!this.dependencyGraph.containsKey(head)) {
                continue;
            }

            for (TransactionId adj: this.dependencyGraph.get(head)) {
                if (adj.equals(head)) {
                    continue;
                } 

                if (!vis.contains(adj)) {
                    vis.add(adj);
                    queue.offer(adj);
                } else {
                    return true; // Deadlock
                }
            }
        }
        return false; // No deadlock
    }
}