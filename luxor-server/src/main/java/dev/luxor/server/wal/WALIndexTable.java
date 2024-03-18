package dev.luxor.server.wal;

/**
 * A {@link WALIndexTable} contains the mapping between a WAL frame and the page residing in this
 * frame.
 *
 * @author houthacker
 */
public interface WALIndexTable {

  /**
   * Stores a mapping from {@code page} to {@code frame} in this table.
   *
   * @param frame The frame number the page resides in.
   * @param page The page number to map.
   */
  void put(int frame, long page);

  /**
   * Finds the number of the frame containing {@code page}.
   *
   * @param page The page number to retrieve.
   * @return The frame index, or {@code -1} if no such frame exists.
   */
  int keyOf(long page);

  /**
   * Returns the amount of elements contained in this table.
   *
   * @return The element count.
   */
  int size();
}
