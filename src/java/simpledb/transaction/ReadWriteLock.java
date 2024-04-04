package simpledb.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReadWriteLock {
    Set<TransactionId> hold;
    Map<TransactionId, Boolean> acqs;
    boolean lockedExclusively; // whether the lock is held exclusively
    private int rnum; // number of transactions holding read locks
    private int wrnum; // number of transactions holding write locks

    // implements an exclusive lock and multiple shared locks
    public ReadWriteLock() {
        this.hold = new HashSet<TransactionId>();
        this.acqs = new HashMap<TransactionId, Boolean>();
        this.lockedExclusively = false; 
        this.rnum = 0; 
        this.wrnum = 0;
    }

    public Set<TransactionId> getHolders() {
        return this.hold;
    }

    public Set<TransactionId> getAcquirers() {
        return this.acqs.keySet();
    }

    public boolean isLockedExclusively() {
        return this.lockedExclusively;
    }

    public boolean lockHeldBy(TransactionId tid) {
        return this.hold.contains(tid);
    }

    public void readLock(TransactionId tid) {
        if (this.hold.contains(tid) && !this.lockedExclusively) {
            return;
        } 

        this.acqs.put(tid, false); // false means read lock
        synchronized (this) {
            try {
                while (this.wrnum != 0) {
                    this.wait();
                }
                ++this.rnum; // rnum is the number of transactions holding read locks
                this.hold.add(tid);
                this.lockedExclusively = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.acqs.remove(tid);
    }

    public void writeLock(TransactionId tid) {
        if (this.hold.contains(tid) && this.lockedExclusively) {
            return;
        } 

        if (this.acqs.containsKey(tid) && this.acqs.get(tid)) {
            return;
        }

        this.acqs.put(tid, true);
        synchronized (this) {
            try {
                if (this.hold.contains(tid)) {
                    while (this.hold.size() > 1) {
                        this.wait();
                    }
                    readUnlockNoNotify(tid);
                }

                while (this.rnum != 0 || this.wrnum != 0) {
                    this.wait();
                }
                ++this.wrnum;
                this.hold.add(tid);
                this.lockedExclusively = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.acqs.remove(tid);
    }

    private void readUnlockNoNotify(TransactionId tid) {
        if (!this.hold.contains(tid)) {
            return;
        }

        synchronized (this) {
            --this.rnum;
            this.hold.remove(tid);
        }
    }

    public void readUnlock(TransactionId tid) {
        if (!this.hold.contains(tid)) {
            return;
        }

        synchronized (this) {
            --this.rnum; 
            this.hold.remove(tid);
            notifyAll();
        }
    }

    public void writeUnlock(TransactionId tid) {
        if (!this.hold.contains(tid)) {
            return;
        }

        if (!this.lockedExclusively) {
            return;
        }

        synchronized (this) {
            --this.wrnum;
            this.hold.remove(tid);
            notifyAll();
        }
    }

    public void unlock(TransactionId tid) {
        if (!this.lockedExclusively) {
            readUnlock(tid);
        }
        else {
            writeUnlock(tid);
        }
    }
}