package dev.luxor.server.wal;

/**
 * A {@link WALIndexHeader} stores a copy of the WAL index metadata. A {@link WALIndexHeader} must
 * always be a copy and must not interact directly with the WAL index state, because doing so would
 * break the ACID properties of the WAL: a transaction is not guaranteed to have a consistent view
 * during its runtime.
 *
 * @author houthacker
 */
public interface WALIndexHeader {

  /**
   * A copy of the random salt of the WAL header.
   *
   * @return The random salt.
   * @see WALHeader#randomSalt()
   */
  int randomSalt();

  /**
   * A copy of the sequential salt of the WAL header.
   *
   * @return The sequential salt.
   * @see WALHeader#sequentialSalt()
   */
  int sequentialSalt();

  /**
   * A copy of the cumulative frame checksum of the WAL header.
   *
   * @return The cumulative checksum.
   * @see WALHeader#checksum()
   */
  long cumulativeChecksum();

  /**
   * Returns the index of the last valid frame within the containing WAL. This frame always contains
   * the latest valid B+Tree root page.
   *
   * @return The last valid frame index, or {@code -1} if there is no such frame.
   */
  int lastCommitFrame();

  /**
   * The database size in pages. This includes the database header page (page {@code 0}).
   *
   * @return The database size in pages, or {@code 0} if the size is unknown.
   */
  long dbSize();

  /**
   * Convenience method to determine if the WAL is empty.
   *
   * @apiNote The returned value is only valid for use if a WAL write lock is held by the current
   *     thread.
   * @return {@code true} if the WAL is empty, {@code false} otherwise.
   */
  default boolean isEmpty() {
    return lastCommitFrame() == -1;
  }

  /**
   * Returns the cursor which indicates the position of the next {@link WALFrame} to write.
   *
   * @return The current wal cursor.
   */
  int cursor();

  /**
   * Notifies this header that the given {@code frame} has been appended to the WAL. The header will
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

  /** Synchronizes the contents of this {@link WALIndexHeader} with its backing storage. */
  void sync();
}
