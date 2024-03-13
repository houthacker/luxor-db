package dev.luxor.server.wal;

/**
 * The type of lock held on a {@link WriteAheadLog}.
 *
 * @author houthacker
 */
public enum WALLockType {

  /** A default value for lock types indicating no lock is currently held. */
  none,

  /** A lock of this type provides clients with shared (read) access to the WAL. */
  shared,

  /** A lock of this type provides clients with exclusive (write) access to the WAL. */
  exclusive
}
