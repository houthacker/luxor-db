package dev.luxor.server.wal;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.*;

import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileLock;
import java.util.ConcurrentModificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link OffHeapWALIndex} stores its data off-heap in a memory-mapped file.
 *
 * @author houthacker
 */
public class OffHeapWALIndex implements WALIndex {

  private static final Logger log = LoggerFactory.getLogger(OffHeapWALIndex.class);

  private static final MemoryLayout HEADERS_LAYOUT =
      MemoryLayout.sequenceLayout(2, OffHeapWALIndexHeader.LAYOUT);

  static {
    assert HEADERS_LAYOUT.byteSize() == 4;
  }

  private static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(
              HEADERS_LAYOUT.withName("header"),
              MemoryLayout.sequenceLayout(8, ValueLayout.JAVA_BYTE).withName("locks"))
          .withName("wal_index");

  private final LuxorFile file;

  private final MemorySegment memory;

  private final OffHeapWALIndexHeader[] headers;

  private final OffHeapHashSet table;

  private WALLockType currentLockType;

  private FileLock processLock;

  /**
   * Creates a new {@link OffHeapWALIndex}. To determine if the index is modified while reading it
   * from memory, an extra copy of the index is stored adjacent to the first one. If these instances
   * are not equal, a concurrent modification is taking place at that time and creating a new {@link
   * OffHeapWALIndex} will fail with an exception.
   *
   * @param file The backing file of this WAL index.
   * @throws ConcurrentModificationException If the WAL index is modified while reading it from
   *     shared memory.
   */
  public OffHeapWALIndex(final LuxorFile file) throws IOException {
    this.memory =
        requireNonNull(file, "Cannot create OffHeapWALIndex: backing file must be non-null.")
            .mapShared(0L, HEADERS_LAYOUT.byteSize());

    final OffHeapWALIndexHeader[] h = loadHeaders(memory);
    if (h[0].equals(h[1])) {
      this.currentLockType = WALLockType.none;
      this.file = file;
      this.headers = h;
      this.table = new OffHeapHashSet(file, LAYOUT.byteSize());
    } else {
      throw new ConcurrentModificationException(
          "Cannot create OffHeapWALIndex: concurrent modification detected.");
    }
  }

  private static OffHeapWALIndexHeader[] loadHeaders(final MemorySegment memory) {
    return new OffHeapWALIndexHeader[] {
      new OffHeapWALIndexHeader(memory.asSlice(0L, OffHeapWALIndexHeader.LAYOUT)),
      new OffHeapWALIndexHeader(
          memory.asSlice(
              LAYOUT.byteOffset(
                  MemoryLayout.PathElement.groupElement("header"),
                  MemoryLayout.PathElement.sequenceElement(1L)),
              OffHeapWALIndexHeader.LAYOUT))
    };
  }

  /**
   * Acquire a shared lock, assuming none is currently held.
   *
   * @param position The file position to lock.
   * @throws LockFailedException If the lock cannot be acquired.
   */
  private void lockShared(long position) throws LockFailedException {
    this.file.readLock().lock();

    try {
      this.file.lock(position, 1L, true);
    } catch (IOException e) {
      this.file.readLock().unlock();

      throw new LockFailedException("Cannot obtain shared lock on WAL index file.", e);
    }
  }

  /**
   * Acquire an exclusive lock, assuming none is currently held.
   *
   * @param position The file position to lock.
   * @throws LockFailedException If the lock cannot be acquired.
   */
  private void lockExclusive(long position) throws LockFailedException {
    this.file.writeLock().lock();

    try {
      this.file.lock(position, 1L, false);
    } catch (IOException e) {
      this.file.writeLock().unlock();

      throw new LockFailedException("Cannot obtain exclusive lock on WAL index file.", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public WALIndexHeader header() {
    return this.headers[0];
  }

  /** {@inheritDoc} */
  @Override
  public boolean isCurrent() {
    // Try to reload any pending changes into the off-heap memory segment.
    this.memory.load();

    final OffHeapWALIndexHeader[] h = loadHeaders(this.memory);
    if (h[0].equals(h[1])) {
      return !this.headers[0].equals(h[0]);
    } else {

      throw new ConcurrentModificationException(
          "Cannot verify OffHeapWALIndex changes: concurrent modification detected.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public int findFrame(final long page) throws IOException {
    return this.table.keyOf(page);
  }

  /** {@inheritDoc} */
  @Override
  public void put(final int frame, final long page) throws IOException {
    this.table.put(ensureAtLeastZero(frame), ensureAtLeastZero(page));
  }

  /** {@inheritDoc} */
  @Override
  public void lock(final WALLockType type) throws LockFailedException {
    requireNonNull(type, "requested lock type must be non-null.");

    if (this.currentLockType.compareTo(type) <= 0) {
      if (this.currentLockType != WALLockType.none) {
        // A lock upgrade is requested, but this is not semantically supported. So, to acquire a
        // more restrictive lock, we first need to release the current lock.
        this.unlock();
      }

      if (type == WALLockType.shared) {
        this.lockShared(type.ordinal());
      } else if (type == WALLockType.exclusive) {
        this.lockExclusive(type.ordinal());
      }
    }

    // We already have a more restrictive lock than the one being requested, so no action required.
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() {

    // First, we attempt to unlock the file (process) lock.
    // This must be done first, because otherwise overlapping locks might be acquired by the current
    // JVM instance (the memory locks are of course already released in that case).
    try {
      this.processLock.release();
    } catch (IOException e) {
      log.warn("Could not unlock WAL index file lock, but must proceed as if it were unlocked", e);
    }

    // Only then release the memory locks.
    if (this.currentLockType == WALLockType.shared) {
      this.file.readLock().unlock();
    } else if (this.currentLockType == WALLockType.exclusive) {
      this.file.writeLock().unlock();
    }

    this.processLock = null;
    this.currentLockType = WALLockType.none;
  }

  /**
   * Reloads the contents of this {@link OffHeapWALIndex}.
   *
   * @throws ConcurrentModificationException If a concurrent modification to the index headers is
   *     detected
   */
  public void reload() {
    final OffHeapWALIndexHeader[] newHeaders = loadHeaders(this.memory);

    if (newHeaders[0].equals(newHeaders[2])) {
      this.headers[0] = newHeaders[0];
      this.headers[1] = newHeaders[1];
      this.table.load();
    } else {
      throw new ConcurrentModificationException(
          "Cannot reload OffHeapWALIndex: concurrent modification detected.");
    }
  }
}
