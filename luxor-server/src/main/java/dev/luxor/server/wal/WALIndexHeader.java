package dev.luxor.server.wal;

/**
 * A WAL index header stores WAL metadata for use by code which traverses or manipulates it.
 *
 * @author houthacker
 */
public interface WALIndexHeader {

  /**
   * Returns the number of the last valid frame within the containing WAL.
   *
   * @return The last valid frame number.
   */
  short lastValidFrame();
}
