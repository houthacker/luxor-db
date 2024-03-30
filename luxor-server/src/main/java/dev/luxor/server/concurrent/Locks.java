package dev.luxor.server.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class contains various utility methods for use in a concurrent environment.
 *
 * @author houthacker
 */
public final class Locks {

  private Locks() {
    throw new UnsupportedOperationException("Locks is a utility class; do not instantiate it.");
  }

  /**
   * Obtains exclusive ownership of all given locks while avoiding deadlocks. After this method has
   * been executed, either all or no locks are owned by the calling thread.<br>
   * This method blocks until all locks have been obtained.
   *
   * @implNote This implementation is the Java-analog of C++'s {@code std::lock} implementation.
   * @param locks The list of locks of which to obtain exclusive ownership.
   */
  public static void exclusiveLock(final ReentrantReadWriteLock... locks) {
    if (locks.length == 0) {
      return;
    }

    int first = 0;
    do {
      locks[first].writeLock().lock();
      for (int j = 1; j < locks.length; ++j) {
        final int idx = (first + j) % locks.length;
        if (!locks[idx].writeLock().tryLock()) {

          // If we fail to lock one of the locks, unlock the previously locked ones.
          for (int k = j; k != 0; --k) {
            locks[first + k - 1 % locks.length].writeLock().unlock();
          }

          first = idx;

          // Exit the for loop and retry from the current index.
          break;
        }
      }
    } while (!locks[first].isWriteLockedByCurrentThread());
  }

  /**
   * Releases exclusive ownership of all provided locks. Any lock that is not exclusively owned by
   * the calling thread is left untouched.
   *
   * @param locks The list of locks of which to release exclusive ownership.
   */
  public static void exclusiveUnlock(final ReentrantReadWriteLock... locks) {
    for (final ReentrantReadWriteLock lock : locks) {
      if (lock.isWriteLockedByCurrentThread()) {
        lock.writeLock().unlock();
      }
    }
  }
}
