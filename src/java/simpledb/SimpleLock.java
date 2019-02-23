package simpledb;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

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

    public HashSet<TransactionId> getOwners() {
        return owns;
    }

    public boolean holdsLock(TransactionId tid) {
        return owns.contains(tid);
    }

    public void release(TransactionId tid) {
        if (!owns.contains(tid) || type == LockType.INIT) {
            return;
        }
        if (type == LockType.EXCLUSIVE) {
            owns.clear();
        } else if (type == LockType.SHARED) {
            owns.remove(tid);
        }
        if (owns.isEmpty()) {
            type = LockType.INIT;
        }
        
        for (TransactionId wait : waits) {
            ArrayList<TransactionId> waitlist = dependencyGraph.get(wait);
            waitlist.remove(tid);
        }
        
        condition.signalAll();
    }

    public boolean isShared(TransactionId tid) {
        if (type == LockType.EXCLUSIVE) {
            return owns.contains(tid); // Can grant shared lock if owns exclusive.
        }
        return true;
    }

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

    public boolean acquireShared(TransactionId tid) {
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

    public boolean acquireExclusive(TransactionId tid) {
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

    public void wait(boolean shared, TransactionId tid) throws TransactionAbortedException {
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
            if (shared) {
                while (!isShared(tid)) {
                    condition.await(1, TimeUnit.SECONDS);
                }
            } else {
                while (!isExclusive(tid)) {
                    condition.await(1, TimeUnit.SECONDS);
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
