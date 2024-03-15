package dev.luxor.server.wal;

import dev.luxor.server.concurrent.LockFailedException;
import java.io.IOException;

/**
 * A WAL index that maps WAL frame numbers to page numbers. Implementations may use backing storage
 * in another type of memory.
 *
 * @author houthacker
 */
public interface WALIndex extends AutoCloseable {

  /**
   * Returns the header of this index.
   *
   * @return The WAL index header.
   */
  WALIndexHeader header();

  /**
   * Returns whether this {@link WALIndex} is current as compared to its main storage in mapped
   * memory. If the implementation does not use secondary storage, this method always returns {@code
   * true}.
   *
   * @return Whether this {@link WALIndex} is up-to-date.
   * @throws java.util.ConcurrentModificationException If a concurrent modification of the headers
   *     is detected.
   */
  boolean isCurrent();

  /**
   * Finds the frame that contains {@code page}.
   *
   * @param page The number of the page that must be contained in the frame.
   * @return The number of the frame containing the given page, or {@code -1} if no such frame
   *     exists.
   */
  int findFrame(final long page);

  /**
   * Maps {@code page} to {@code frame} so it can be searched for later.
   *
   * @param frame The frame number to map the page number to.
   * @param page The page number to map.
   * @throws IOException If an I/O error occurs while storing the page mapping.
   */
  void put(final int frame, final long page) throws IOException;

  /**
   * Acquires a lock of {@code type} on this index. These locks will control concurrency within the
   * JVM as well as between processes competing for conflicting locks.
   *
   * <p>If a lock of {@code type} is already held by the calling thread, this method will have no
   * (additional) effect.
   *
   * @param type The lock type to acquire.
   * @throws LockFailedException If the requested lock cannot be obtained.
   */
  void lock(final WALLockType type) throws LockFailedException;

  /**
   * Releases a currently held lock on this index.<br>
   * If no lock is currently held, this method has no effect.
   */
  void unlock();
}
