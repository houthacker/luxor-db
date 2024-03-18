package dev.luxor.server.wal;

import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.concurrent.OutOfOrderLockException;
import java.io.Closeable;
import java.io.IOException;

/**
 * A WAL index that maps WAL frame numbers to page numbers. Implementations may use backing storage
 * in another type of memory.
 *
 * @author houthacker
 */
public interface WALIndex extends Closeable {

  /**
   * Returns the header of this index.
   *
   * @return The WAL index header.
   */
  WALIndexHeader header();

  /**
   * Returns whether this {@link WALIndex} is outdated as compared to its main storage in mapped
   * memory. If the implementation does not use secondary storage, this method always returns {@code
   * false}.
   *
   * @return Whether this {@link WALIndex} is outdated.
   * @throws java.util.ConcurrentModificationException If a concurrent modification of the headers
   *     is detected.
   */
  boolean isStale();

  /**
   * Finds the frame that contains {@code page}.
   *
   * @param page The number of the page that must be contained in the frame.
   * @return The number of the frame containing the given page, or {@code -1} if no such frame
   *     exists.
   */
  int findFrame(long page);

  /**
   * Maps {@code page} to {@code frame} so it can be searched for later.
   *
   * @param frame The frame number to map the page number to.
   * @param page The page number to map.
   * @throws IOException If an I/O error occurs while storing the page mapping.
   */
  void put(int frame, long page) throws IOException;

  /**
   * Returns the most restrictive lock currently held by this {@link WALIndex}.
   *
   * @return The current lock type.
   */
  WALLockType currentLock();

  /**
   * Acquires a lock of {@code type} on this index. These locks control concurrency within the JVM
   * as well as between competing processes.
   *
   * <p>A shared lock must be held prior to requesting an exclusive lock.
   *
   * <p>If a lock of {@code type} is already held by the calling thread, this method will have no
   * (additional) effect.
   *
   * @param type The lock type to acquire.
   * @throws OutOfOrderLockException If an exclusive lock is requested, but no shared lock is
   *     currently held.
   * @throws LockFailedException If the requested lock cannot be obtained.
   * @see OutOfOrderLockException
   */
  void lock(WALLockType type) throws LockFailedException;

  /** Releases any locks currently held. If no lock is currently held, this method has no effect. */
  void unlock();
}
