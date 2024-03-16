package dev.luxor.server.wal;

/**
 * The type of lock held on a {@link WriteAheadLog}.
 *
 * @author houthacker
 */
public enum WALLockType {

  /** A default value for lock types indicating no lock is currently held. */
  NONE(0x01),

  /** A lock of this type provides clients with shared (read) access to the WAL. */
  SHARED(0x02),

  /** A lock of this type provides clients with exclusive (write) access to the WAL. */
  EXCLUSIVE(0x04);

  /** The lock mask. */
  private final int mask;

  WALLockType(final int mask) {
    this.mask = mask;
  }

  /**
   * Returns the mask of this lock type.
   *
   * @return The mask of this lock type.
   */
  public int mask() {
    return this.mask;
  }

  /**
   * Returns file offset to lock for this lock type.
   *
   * @return The file offset.
   */
  public int offset() {
    return mask - 2;
  }
}
