package dev.luxor.server.wal;

import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.concurrent.OutOfOrderLockException;
import java.io.Closeable;

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
   * Searches for the index of the frame that contains {@code page}.
   *
   * @param page The index of the page that must be contained in the frame.
   * @return The index of the frame containing the given page, or {@code -1} if no such frame
   *     exists.
   */
  int findFrameIndexOf(long page);

  /**
   * Notifies this index that the given {@code frame} has been appended to the WAL. The index will
   * then update itself accordingly:
   *
   * <ul>
   *   <li>The cursor position will be incremented
   *   <li>The cumulative checksum will be updated
   *   <li>If the frame is a commit frame, the last commit frame will be set to the given {@code
   *       index}
   * </ul>
   *
   * @param frame The {@link WALFrame} that was appended.
   * @param frameIndex The index the frame was written at.
   */
  void notifyAppended(WALFrame frame, int frameIndex);

  /** Synchronizes this {@link WALIndex} with its backing storage. */
  void sync();

  /**
   * Returns the most restrictive lock currently held by this {@link WALIndex}.
   *
   * @return The current lock type.
   */
  WALLockType currentLock();

  /**
   * Obtains a lock of {@code type} on this index. Implementations must ensure that locks obtained
   * with this method control concurrency within the JVM as well as between competing processes.
   *
   * <p>A shared lock must be held prior to requesting an exclusive lock.
   *
   * <p>If a lock of {@code type} is already held by the calling thread, this method will have no
   * (additional) effect.
   *
   * @param type The lock type to obtain.
   * @throws NullPointerException If {@code type} is {@code null}.
   * @throws OutOfOrderLockException If an exclusive lock is requested, but no shared lock is
   *     currently held.
   * @throws LockFailedException If the requested lock cannot be obtained.
   * @see OutOfOrderLockException
   */
  void lock(WALLockType type) throws LockFailedException;

  /**
   * Tries to obtain a lock of {@code type} on this index. Implementations must ensure that locks
   * obtained using this method control concurrency within the JVM as well as between competing
   * processes.
   *
   * <p>A shared lock must be held prior to requesting an exclusive lock.
   *
   * <p>If a lock of {@code type} is already held by the calling thread, this method will have no
   * additional effect and immediately return {@code true}.
   *
   * @param type The lock type to obtain.
   * @return {@code true} if the requested lock is now held by the current thread, {@code false}
   *     otherwise.
   * @throws NullPointerException If {@code type} is {@code null}.
   * @throws OutOfOrderLockException If an exclusive lock is requested, but no shared lock is held
   *     by the calling thread.
   * @see OutOfOrderLockException
   */
  boolean tryLock(WALLockType type) throws OutOfOrderLockException;

  /** Releases any locks currently held. If no lock is currently held, this method has no effect. */
  void unlock();
}
