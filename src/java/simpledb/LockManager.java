package simpledb;

import java.util.*;
import java.util.concurrent.locks.*;

// LockManager manages locks in simpleDB
class LockManager {
    // fields
    private final Lock locker;
    private final HashMap<PageId, SimpleLock> locks;
    private final HashMap<TransactionId, ArrayList<TransactionId>> dependencyGraph;

    public LockManager() {
        locker = new ReentrantLock();
        locks = new HashMap<PageId, SimpleLock>();
        dependencyGraph = new HashMap<TransactionId, ArrayList<TransactionId>>();
    }
    
    public void acquireShared(TransactionId tid, PageId pid) throws TransactionAbortedException {
        locker.lock();
        
        SimpleLock slock = locks.get(pid);

        if (slock == null) {
            slock = new SimpleLock(locker, dependencyGraph);
            locks.put(pid, slock);
        }

        while (!slock.acquireShared(tid)) {
            if (detectDeadLock(slock, tid)) {
                locker.unlock();
                // deadlock occurs, throws TAE exception
                throw new TransactionAbortedException();
            } else {
                slock.wait(true, tid);
            }
        }
        
        locker.unlock();
    }

    public void acquireExclusive(TransactionId tid, PageId pid) throws TransactionAbortedException {
        locker.lock();

        SimpleLock slock = locks.get(pid);

        if (slock == null) {
            slock = new SimpleLock(locker, dependencyGraph);
            locks.put(pid, slock);
        }

        while (!slock.acquireExclusive(tid)) {
            if (detectDeadLock(slock, tid)) {
                locker.unlock();
                // deadlock occurs, throws TAE exception
                throw new TransactionAbortedException();
            } else {
                slock.wait(false, tid);
            }
        }
        
        locker.unlock();
    }
    
    public boolean holdsLock(TransactionId tid, PageId pid) {
        locker.lock();

        try {
            SimpleLock slock = locks.get(pid);

            if (slock == null) {
                return false;
            }
            
            return slock.holdsLock(tid);
        } finally {
            locker.unlock();
        }
    }

    public void release(TransactionId tid, PageId pid) {
        locker.lock();

        try {
            SimpleLock slock = locks.get(pid);

            if (slock != null) {
                slock.release(tid);
            }
        } finally {
            locker.unlock();
        }
    }

    public void releaseAll(TransactionId tid) {
        locker.lock();

        try {
            for (SimpleLock slock : locks.values()) {
                slock.release(tid);
            }
        } finally {
            locker.unlock();
        }
    }
    
    // check connectivity from s to t in the dependency graph
    private boolean isConnected(TransactionId from, TransactionId to) {
        // BFS search
        Queue<TransactionId> q = new LinkedList<TransactionId>();
        HashSet<TransactionId> visited = new HashSet<TransactionId>();

        q.add(from);
        visited.add(from);
        
        while (!q.isEmpty()) {
            TransactionId current = q.poll();
            ArrayList<TransactionId> neighbors = dependencyGraph.get(current);

            if (neighbors == null) { continue; }
            
            for (TransactionId neightbor : neighbors) {
                // a cycle is detected - deadlock
                if (neightbor.equals(to)) {
                    return true;
                }
                if (!visited.contains(neightbor)) {
                    q.add(neightbor);
                    visited.add(neightbor);
                }
            }
        }
        return false;
    }

    private boolean detectDeadLock(SimpleLock slock, TransactionId wait) {
        for (TransactionId own : slock.getOwners()) {
            // there is a cycle in the dependency graph
            if (isConnected(own, wait)) {
                return true;
            }
        }
        return false;
    }
}