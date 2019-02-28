package simpledb;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

// SimpleLock is a class that represents a lock in SimpleDB
public class SimpleLock {
    // fields of SimpleLock
    private LockType type;
    private final Condition condition;
    private final HashSet<TransactionId> owns;
    private final HashSet<TransactionId> waits;
    // dependency dependencyGraph to detect cycle
    private final HashMap<TransactionId, ArrayList<TransactionId>> dependencyGraph;

    public SimpleLock(Lock m, HashMap<TransactionId, ArrayList<TransactionId>> dependencyGraph) {
        this.type = LockType.INIT;
        this.condition = m.newCondition();
        this.owns = new HashSet<TransactionId>();
        this.waits = new HashSet<TransactionId>();
        this.dependencyGraph = dependencyGraph;
    }

    // get owners of this lock
    public HashSet<TransactionId> getOwners() {
        return owns;
    }

    public boolean holdsLock(TransactionId tid) {
        return owns.contains(tid);
    }

    // release lock that associates with transaction
    public void release(TransactionId tid) {
        // nothing happens when the lock type is INIT
        if (!owns.contains(tid) || type == LockType.INIT) {
            return;
        }
        // release exclusive lock, make owns empty (one transaction held)
        if (type == LockType.EXCLUSIVE) {
            owns.clear();
        // release shared lock, remove only the associated transaction in owns
        } else if (type == LockType.SHARED) {
            owns.remove(tid);
        }
        // owns is empty, now the type of lock is INIT
        if (owns.isEmpty()) {
            type = LockType.INIT;
        }
        
        // remove the associated transaction in waitlist
        for (TransactionId wait : waits) {
            ArrayList<TransactionId> waitlist = dependencyGraph.get(wait);
            waitlist.remove(tid);
        }
        
        // wakes up all thread, which are waiting
        condition.signalAll();
    }

    // check if shared lock can be granted
    public boolean isShared(TransactionId tid) {
        if (type == LockType.EXCLUSIVE) {
            return owns.contains(tid);
        }
        return true;
    }
    
    // check if exclusive lock can be granted
    public boolean isExclusive(TransactionId tid) {
        if (type == LockType.EXCLUSIVE) {
            return owns.contains(tid);
            
        } else if (type == LockType.SHARED) {
            if (owns.size() == 1 && owns.contains(tid)) {
                return true;
            }
            return false;
        }
        return true;
    }

    // acquire shared lock
    public boolean acquireShared(TransactionId tid) {
        // check availability
        if (!isShared(tid)) {
            return false;
        }
        type = LockType.SHARED;
        
        if (!owns.contains(tid)) {
            owns.add(tid);
            
            for (TransactionId wait : waits) {
                ArrayList<TransactionId> waitlist = dependencyGraph.get(wait);
                
                if (waitlist == null) {
                    waitlist = new ArrayList<TransactionId>();
                    dependencyGraph.put(wait, waitlist);
                }
                
                waitlist.add(tid);
            }
        }
        return true;
    }

    // acquire exclusive lock
    public boolean acquireExclusive(TransactionId tid) {
     // check availability
        if (!isExclusive(tid)) {
            return false;
        }
        type = LockType.EXCLUSIVE;
        
        if (!owns.contains(tid)) {
            owns.add(tid);
            
            for (TransactionId wait : waits) {
                
                ArrayList<TransactionId> waitlist = dependencyGraph.get(wait);
                
                if (waitlist == null) {
                    waitlist = new ArrayList<TransactionId>();
                    dependencyGraph.put(wait, waitlist);
                }
                
                waitlist.add(tid);
            }
        }
        return true;
    }
    
    // wait method is used in LockManager when a lock acquiring request occurs
    public void wait(boolean isShared, TransactionId tid) throws TransactionAbortedException {
        if (!waits.contains(tid)) {
            waits.add(tid);
            ArrayList<TransactionId> waitlist = dependencyGraph.get(tid);
            
            if (waitlist == null) {
                waitlist = new ArrayList<TransactionId>();
                dependencyGraph.put(tid, waitlist);
            }
            
            for (TransactionId own : owns) {
                waitlist.add(own);
            }
        }
        
        try {
            // the type of lock is a shared lock
            if (isShared) {
                while (!isShared(tid)) {
                    condition.await(100, TimeUnit.MILLISECONDS);
                }
            // the type of lock is a exclusive lock
            } else {
                while (!isExclusive(tid)) {
                    condition.await(100, TimeUnit.MILLISECONDS);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new TransactionAbortedException();
            
        } finally {
            waits.remove(tid);
            ArrayList<TransactionId> waitlist = dependencyGraph.get(tid);
            
            for (TransactionId own : owns) {
                waitlist.remove(own);
            }
        }
    }
}
