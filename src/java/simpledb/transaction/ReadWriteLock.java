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
        hold = new HashSet<TransactionId>();
        acqs = new HashMap<TransactionId, Boolean>();
        lockedExclusively = false; 
        rnum = 0; 
        wrnum = 0;
    }

    public void readLock(TransactionId tid) {
        if (hold.contains(tid) && !lockedExclusively) {
            return;
        } 

        acqs.put(tid, false); // false means read lock
        synchronized (this) {
            try {
                while (wrnum != 0) {
                    this.wait();
                }
                ++rnum; // rnum is the number of transactions holding read locks
                hold.add(tid);
                lockedExclusively = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acqs.remove(tid);
    }

    public void writeLock(TransactionId tid) {
        if (hold.contains(tid) && lockedExclusively) {
            return;
        } 

        if (acqs.containsKey(tid) && acqs.get(tid)) {
            return;
        }

        acqs.put(tid, true);
        synchronized (this) {
            try {
                if (hold.contains(tid)) {
                    while (hold.size() > 1) {
                        this.wait();
                    }
                    readUnlockWithoutNotifyingAll(tid);
                }

                while (rnum != 0 || wrnum != 0) {
                    this.wait();
                }
                ++wrnum;
                hold.add(tid);
                lockedExclusively = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acqs.remove(tid);
    }

    private void readUnlockWithoutNotifyingAll(TransactionId tid) {
        if (!hold.contains(tid)) {
            return;
        }

        synchronized (this) {
            --rnum;
            hold.remove(tid);
        }
    }

    public void readUnlock(TransactionId tid) {
        if (!hold.contains(tid)) {
            return;
        }

        synchronized (this) {
            --rnum; 
            hold.remove(tid);
            notifyAll();
        }
    }

    public void writeUnlock(TransactionId tid) {
        if (!hold.contains(tid)) {
            return;
        }

        if (!lockedExclusively) {
            return;
        }

        synchronized (this) {
            --wrnum;
            hold.remove(tid);
            notifyAll();
        }
    }

    public void unlock(TransactionId tid) {
        if (!lockedExclusively) {
            readUnlock(tid);
        }
        else {
            writeUnlock(tid);
        }
    }

    public Set<TransactionId> holders() {
        return hold;
    }

    public boolean isLockedExclusively() {
        return lockedExclusively;
    }

    public Set<TransactionId> acquirers() {
        return acqs.keySet();
    }

    public boolean heldBy(TransactionId tid) {
        return holders().contains(tid);
    }
}