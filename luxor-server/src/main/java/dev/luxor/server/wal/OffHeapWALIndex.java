package dev.luxor.server.wal;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.requireNonNull;

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
    // If the byte size changes, the main index layout might require padding.
    assert HEADERS_LAYOUT.byteSize() == 4;
  }

  /** The amount of lock types. Each lock type has its own lock byte in the backing file. */
  private static final int FILE_LOCK_COUNT = 8;

  private static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(
              HEADERS_LAYOUT.withName("header"),
              MemoryLayout.sequenceLayout(FILE_LOCK_COUNT, ValueLayout.JAVA_BYTE).withName("locks"))
          .withName("wal_index");

  static {

    // Since the alignment requirement of the index layout is so small, we don't really have to
    // take care of our writes to the related MemorySegment. If this alignment changes in the
    // future, we might have to rethink this. The OffHeapHashSet already does this, so that can be
    // used as an example.
    assert LAYOUT.byteAlignment() == 2;
  }

  /** The backing file. */
  private final LuxorFile file;

  /** The memory segment containing the WAL index header and lock bytes. */
  private final MemorySegment memory;

  /** An on-heap copy of the wal index headers. */
  private final OffHeapWALIndexHeader[] headers;

  /** The table containing the frame/page mappings. */
  private final OffHeapHashSet table;

  /** A bit-masked integer storing the lock types held at a given point in time. */
  private int locks;

  /** The file locks, one entry per lock type, corresponding to the lock bytes. */
  private final FileLock[] fileLocks;

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
            .mapShared(0L, LAYOUT.byteSize());

    final OffHeapWALIndexHeader[] h = loadHeaders(memory);
    if (h[0].equals(h[1])) {
      this.locks = WALLockType.NONE.mask();
      this.fileLocks = new FileLock[FILE_LOCK_COUNT];
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
   * Returns whether a less restrictive lock than {@code requested} is already held.
   *
   * @param requested The requested lock type.
   * @return {@code true} if a less restrictive lock is held, {@code false} otherwise.
   */
  private boolean lockedLessRestrictiveThan(final WALLockType requested) {
    return this.locks < requested.mask();
  }

  /**
   * Returns whether a lock of the given type is currently held.
   *
   * @param type The lock type to check.
   * @return {@code true} if a lock of the given type is currently held, {@code false} otherwise.
   */
  private boolean hasLock(final WALLockType type) {
    return (this.locks & type.mask()) == type.mask();
  }

  /**
   * Acquire a shared lock on the WAL index.
   *
   * @throws LockFailedException If the lock cannot be acquired.
   */
  private void lockShared() throws LockFailedException {
    if (!this.hasLock(WALLockType.SHARED)) {
      this.file.mutex().readLock().lock();

      try {
        final int position = WALLockType.SHARED.offset();
        this.fileLocks[position] = this.file.lock(position, 1L, true);
        this.locks |= WALLockType.SHARED.mask();
      } catch (IOException e) {
        this.file.mutex().readLock().unlock();
        throw new LockFailedException("Cannot obtain shared lock on WAL index file.", e);
      }
    }
  }

  /** Release a shared lock on the WAL index. If no lock is held, this method has no effect. */
  private void unlockShared() {
    if (this.hasLock(WALLockType.SHARED)) {
      final int position = WALLockType.SHARED.offset();
      try {
        this.fileLocks[position].release();
      } catch (IOException e) {
        log.warn(
            "Could not unlock shared WAL index file lock, but must proceed as if it were unlocked.",
            e);
      }

      this.fileLocks[position] = null;
      this.file.mutex().readLock().unlock();
      this.locks ^= WALLockType.SHARED.mask();
    }
  }

  /**
   * Acquire an exclusive lock.
   *
   * @throws LockFailedException If the lock cannot be acquired.
   */
  private void lockExclusive() throws LockFailedException {
    if (!this.hasLock(WALLockType.EXCLUSIVE)) {

      // To acquire an exclusive lock, a shared lock must also be held, because a shared lock
      // prevents
      // a checkpoint from being executed.
      // To support lock upgrades (a read transaction might become a write transaction), if no
      // shared
      // lock is held at this time, one will be acquired.
      this.lock(WALLockType.SHARED);

      this.file.lock().lock();
      try {
        final int position = WALLockType.EXCLUSIVE.offset();
        this.fileLocks[position] = this.file.lock(position, 1L, false);
        this.locks |= WALLockType.EXCLUSIVE.mask();
      } catch (IOException e) {
        this.file.lock().unlock();

        throw new LockFailedException("Cannot obtain exclusive lock on WAL index file.", e);
      }
    }
  }

  /** Release an exclusive lock. If no exclusive lock is held, this method has no effect. */
  private void unlockExclusive() {
    if (this.hasLock(WALLockType.EXCLUSIVE)) {
      final int position = WALLockType.EXCLUSIVE.offset();
      try {
        this.fileLocks[position].release();
      } catch (IOException e) {
        log.warn(
            "Could not unlock shared WAL index file lock, but must proceed as if it were unlocked.",
            e);
      }

      this.fileLocks[position] = null;
      this.file.lock().unlock();
      this.locks ^= WALLockType.EXCLUSIVE.mask();
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
  public int findFrame(final long page) {
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

    if (this.lockedLessRestrictiveThan(type)) {
      if (type == WALLockType.SHARED) {
        this.lockShared();
      } else if (type == WALLockType.EXCLUSIVE) {
        this.lockExclusive();
      }
    }

    // We already have a more restrictive lock than the one being requested, so no action required.
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() {
    if (this.locks != WALLockType.NONE.mask()) {

      // If we're locked (as indicated by this.locks), then the shared lock is always held.
      // If the exclusive lock is held as well, unlock that first, since otherwise a checkpoint may
      // start before the exclusive lock has been released.
      if (this.hasLock(WALLockType.EXCLUSIVE)) {
        this.unlockExclusive();
      }

      this.unlockShared();
    }
  }

  /**
   * Reloads the header of this {@link OffHeapWALIndex} from memory.
   *
   * @throws ConcurrentModificationException If a concurrent modification to the index headers is
   *     detected
   */
  public void reload() {
    final OffHeapWALIndexHeader[] newHeaders = loadHeaders(this.memory);

    if (newHeaders[0].equals(newHeaders[2])) {
      this.headers[0] = newHeaders[0];
      this.headers[1] = newHeaders[1];
      this.table.reload();
    } else {
      throw new ConcurrentModificationException(
          "Cannot reload OffHeapWALIndex: concurrent modification detected.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    this.unlock();
    this.file.close();
  }
}
