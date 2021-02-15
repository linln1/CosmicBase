package mvcc.concur;

/**
 * A runtime exception indicating that the transaction needs to abort
 * because a lock could not be obtained.
 */
public class LockAbortException extends RuntimeException {
    public LockAbortException() {
    }
}