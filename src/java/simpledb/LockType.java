package simpledb;

// LockType represents a type of SimpleLock
public class LockType {
    int lockType;

    private LockType(int lockType) {
      this.lockType = lockType;
    }

    public String toString() {
      if (lockType == 0) {
          return "INIT";
      }
      if (lockType == 1) {
          return "EXCLUSIVE";
      }
      if (lockType > 1) {
          return "SHARED";
      }
      return "UNKNOWN";
    }

    public static final LockType INIT = new LockType(0);
    public static final LockType EXCLUSIVE = new LockType(1);
    public static final LockType SHARED = new LockType(2);
}
